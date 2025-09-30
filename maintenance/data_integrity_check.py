import os
import collections
import concurrent.futures
import pandas as pd
import polars as pl
from datetime import datetime
from tqdm import tqdm
import logging
import sys
import time
import pytz

# Import project core tools
try:
    # Assuming the project structure is set up such that project_core is importable
    from project_core import file_manager
except ImportError as e:
    logging.error(
        f"Failed to import project_core. Ensure project root is set as Sources Root in PyCharm or added to PYTHONPATH. Error: {e}")
    # Attempting a fallback relative import if the environment is not configured
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from project_core import file_manager
    except ImportError:
        logging.error("Fallback import failed. Exiting.")
        sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

# --- Configuration and Resource Throttling ---

# 1. Resource Management (Throttling and Batching)
# These settings are optimized based on previous runs to prevent resource exhaustion.
MAX_WORKERS = 6
os.environ['POLARS_MAX_THREADS'] = '4'
BATCH_SIZE = 50

# 2. Analysis Parameters
# Define the threshold for an anomalous gap during RTH (in seconds)
ANOMALOUS_GAP_THRESHOLD_SEC = 15 * 60  # 15 minutes

# Define Regular Trading Hours (RTH) in Eastern Time
# "US/Eastern" correctly handles historical Daylight Saving Time changes.
MARKET_TIMEZONE = pytz.timezone("US/Eastern")
RTH_START_TIME = datetime.strptime("09:30", "%H:%M").time()
RTH_END_TIME = datetime.strptime("16:00", "%H:%M").time()

# 3. Schema Mapping (Raw Polygon Tick Schema)
TIMESTAMP_COL = "sip_timestamp"


# ---------------------------------------------

def discover_tickers(base_path):
    """Discovers all tickers and their 'tick' fidelity Parquet files efficiently."""
    # (This function is identical to the optimized one used in liquidity_screener.py)
    trading_history_root = os.path.join(base_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        logging.error(f"Trading history directory not found at: {trading_history_root}")
        return {}

    tickers_files = collections.defaultdict(list)
    logging.info(f"Scanning for 'tick' data in: {trading_history_root}")

    try:
        with os.scandir(trading_history_root) as entries:
            for entry in entries:
                if entry.is_dir():
                    ticker_folder = entry.name
                    tick_path = os.path.join(entry.path, "tick")
                    if os.path.isdir(tick_path):
                        try:
                            with os.scandir(tick_path) as tick_entries:
                                for tick_entry in tick_entries:
                                    if tick_entry.is_file() and tick_entry.name.endswith(".parquet"):
                                        tickers_files[ticker_folder].append(tick_entry.path)
                        except Exception as e:
                            logging.warning(f"Could not scan directory {tick_path}: {e}")
    except Exception as e:
        logging.error(f"Error during directory scan: {e}")
        return {}

    logging.info(f"Discovered {len(tickers_files)} tickers with tick data.")
    return tickers_files


def analyze_ticker_integrity(ticker, parquet_files):
    """
    Analyzes the Parquet files for a single ticker to identify gaps and anomalies during RTH.
    """
    try:
        # 1. Initialize Polars LazyFrame (Scan Parquet)
        # Select only the timestamp column for efficiency.
        df_lazy = pl.scan_parquet(parquet_files).select([TIMESTAMP_COL])

        # 2. Timestamp Conversion and Timezone Handling
        # Polygon timestamps (ns integers) are UTC. We must convert them to ET to define RTH.
        df_lazy = df_lazy.with_columns(
            pl.col(TIMESTAMP_COL)
            .cast(pl.Datetime(time_unit="ns"))
            .dt.replace_time_zone("UTC")  # Tell Polars the source is UTC
            .dt.convert_time_zone(str(MARKET_TIMEZONE))  # Convert to Eastern Time (handles DST)
            .alias("datetime_et")
        )

        # 3. Analyze Overall Coverage (Before RTH filtering)
        coverage_lazy = df_lazy.select(
            pl.col("datetime_et").min().alias("start_date"),
            pl.col("datetime_et").max().alias("end_date")
        )

        # 4. Filter for Regular Trading Hours (RTH)
        # We filter using the time component of the datetime_et column.
        df_rth_lazy = df_lazy.filter(
            (pl.col("datetime_et").dt.time() >= RTH_START_TIME) &
            (pl.col("datetime_et").dt.time() <= RTH_END_TIME)
        )

        # 5. Gap and Volume Analysis (Grouped by Day)
        daily_analysis_lazy = (
            df_rth_lazy
            .sort("datetime_et")
            # Group by the date component of the ET timestamp
            .group_by(pl.col("datetime_et").dt.date().alias("date"))
            .agg(
                pl.count().alias("daily_rth_trades"),
                # Calculate time differences between consecutive trades (in seconds)
                pl.col("datetime_et").diff().dt.total_seconds().alias("time_diffs")
            )
            .with_columns(
                # Analyze the list of time differences for each day
                # Count how many gaps within the day exceed the threshold
                pl.col("time_diffs").list.eval(
                    pl.element().filter(pl.element() > ANOMALOUS_GAP_THRESHOLD_SEC).count()
                ).alias("significant_gaps_count"),
                # Find the maximum gap duration within the day
                pl.col("time_diffs").list.max().alias("max_gap_sec")
            )
            .select(["date", "daily_rth_trades", "significant_gaps_count", "max_gap_sec"])
        )

        # Execute the queries using the streaming engine
        # We execute coverage and daily_analysis separately
        coverage = coverage_lazy.collect(streaming=True)
        daily_analysis = daily_analysis_lazy.collect(streaming=True)

        if daily_analysis.is_empty():
            return {"ticker": ticker, "status": "No RTH Data Found"}

        # 6. Summarize Findings
        total_days = len(daily_analysis)

        # Identify days with zero trades during RTH (indicates potential data dropout)
        zero_trade_days = daily_analysis.filter(pl.col("daily_rth_trades") == 0)

        # Identify days with significant intraday gaps
        days_with_gaps = daily_analysis.filter(pl.col("significant_gaps_count") > 0)

        # Determine overall status
        status = "Healthy"
        if len(zero_trade_days) > 0 or len(days_with_gaps) > 0:
            status = "Anomalies Detected"

        summary = {
            "ticker": ticker,
            "status": status,
            "start_date_et": coverage["start_date"][0].strftime('%Y-%m-%d') if coverage["start_date"][0] else None,
            "end_date_et": coverage["end_date"][0].strftime('%Y-%m-%d') if coverage["end_date"][0] else None,
            "total_trading_days_rth": total_days,
            "days_zero_rth_volume": len(zero_trade_days),
            "days_with_significant_gaps": len(days_with_gaps),
            # Calculate the maximum gap found across all days, rounded for readability
            "max_intraday_gap_sec": round(days_with_gaps["max_gap_sec"].max(), 2) if len(days_with_gaps) > 0 else 0,
        }

        return summary

    except Exception as e:
        # Handle potential resource errors or Rust panics during processing
        error_message = str(e)
        if "PanicException" in error_message or "Out of memory" in error_message or "could not spawn threads" in error_message or "paging file" in error_message:
            error_message = f"System Resource Error (OOM/Panic): {error_message[:150]}..."
        return {"ticker": ticker, "status": "Error", "error": error_message}


def run_data_integrity_check():
    """Main function to orchestrate the integrity check using batching."""
    logging.info("--- LAUNCHING DATA INTEGRITY CHECK WORKFLOW ---")
    start_time = time.time()

    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        logging.error("Base output path not found in configuration.")
        return

    # 1. Discovery
    tickers_files_dict = discover_tickers(base_output_path)
    if not tickers_files_dict:
        return

    tickers_files_list = list(tickers_files_dict.items())
    total_tickers = len(tickers_files_list)
    all_results = []

    logging.info(
        f"\n--- Starting Analysis (Gap Threshold: {ANOMALOUS_GAP_THRESHOLD_SEC / 60:.1f} min, Total Tickers: {total_tickers}) ---")
    logging.info(
        f"Resource Configuration: MAX_WORKERS={MAX_WORKERS}, POLARS_MAX_THREADS={os.environ['POLARS_MAX_THREADS']}, BATCH_SIZE={BATCH_SIZE}")

    # Initialize the progress bar
    pbar = tqdm(total=total_tickers, desc="Analyzing Tickers")

    # 2. Batch Processing
    for i in range(0, total_tickers, BATCH_SIZE):
        batch = tickers_files_list[i:i + BATCH_SIZE]

        # Use ProcessPoolExecutor within the loop (context manager) to ensure resources are released after each batch.
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_ticker = {}

            for ticker, files in batch:
                future = executor.submit(analyze_ticker_integrity, ticker, files)
                future_to_ticker[future] = ticker

            # Process results for the current batch
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    result = future.result()
                    all_results.append(result)
                except concurrent.futures.process.BrokenProcessPool:
                    logging.error(f"Process pool broke near {ticker}. Severe system resource issue. Aborting.")
                    pbar.close()
                    return
                except Exception as e:
                    error_msg = str(e)
                    if "Can't pickle" in error_msg:
                        error_msg = "Process crashed unexpectedly (likely resource exhaustion/Rust panic)."
                    logging.error(f"Error retrieving result for {ticker}: {error_msg}")
                    all_results.append({"ticker": ticker, "status": "Error", "error": error_msg})

                pbar.update(1)

    pbar.close()

    # 3. Reporting
    logging.info("\n--- Generating Report ---")
    df_results = pd.DataFrame(all_results)

    # Ensure numeric columns exist and are properly typed for sorting
    numeric_cols = ['days_zero_rth_volume', 'days_with_significant_gaps', 'max_intraday_gap_sec']
    for col in numeric_cols:
        if col in df_results.columns:
            df_results[col] = pd.to_numeric(df_results[col], errors='coerce').fillna(0)

    # Sort by status (Errors/Anomalies first), then by the severity of issues
    if 'days_with_significant_gaps' in df_results.columns and 'days_zero_rth_volume' in df_results.columns:
        # Create a custom sort order for status
        status_order = {'Error': 1, 'Anomalies Detected': 2, 'No RTH Data Found': 3, 'Healthy': 4}
        df_results['status_rank'] = df_results['status'].map(status_order).fillna(99)

        # Prioritize tickers with the most gaps and zero volume days
        df_results = df_results.sort_values(
            by=["status_rank", "days_with_significant_gaps", "days_zero_rth_volume"],
            ascending=[True, False, False]
        ).drop(columns=['status_rank'])

    # Define output path
    output_dir = os.path.join(base_output_path, "stocks", "maintenance")
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "data_integrity_report.csv")

    # Use file manager helper if available
    try:
        unique_report_path = file_manager.get_unique_filepath(report_path)
    except (AttributeError, NameError):
        logging.warning("file_manager.get_unique_filepath not found or failed. Using default path.")
        unique_report_path = report_path

    # Save the report
    try:
        df_results.to_csv(unique_report_path, index=False)
        logging.info(f"Successfully saved integrity report to: {unique_report_path}")
    except Exception as e:
        logging.error(f"Failed to save report: {e}")

    # Summary statistics
    if 'status' in df_results.columns:
        summary = df_results['status'].value_counts()
        logging.info("\n--- Summary Statistics ---")
        print(summary)

    end_time = time.time()
    logging.info(
        f"\n--- DATA INTEGRITY CHECK WORKFLOW FINISHED (Total Time: {(end_time - start_time) / 60:.2f} minutes) ---")


if __name__ == "__main__":
    # Configure Polars streaming engine
    try:
        pl.Config.set_streaming_chunk_size(1_000_000)
    except Exception:
        pass

    run_data_integrity_check()