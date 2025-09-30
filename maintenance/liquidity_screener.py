import os
import collections
import concurrent.futures
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import sys
import time

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
LOOKBACK_DAYS = 365 * 2  # Analyze the last 2 years

# 1. Throttle Python Parallelism (Process Count)
# Reduce the number of concurrent Python processes to lower memory pressure.
MAX_WORKERS = 6

# 2. Limit Polars Internal Threads (Threads per Process)
# Prevent thread explosion by limiting threads used by Polars *per process*.
os.environ['POLARS_MAX_THREADS'] = '4'

# 3. Batch Processing
# Process tickers in smaller batches to manage memory footprint and allow resource release.
BATCH_SIZE = 50

logging.info(
    f"Resource Configuration: MAX_WORKERS={MAX_WORKERS}, POLARS_MAX_THREADS={os.environ['POLARS_MAX_THREADS']}, BATCH_SIZE={BATCH_SIZE}")

# ---------------------------------------------

# Define the Liquidity Tiers (Criteria)
TIERS = {
    1: {"ADDV_MIN": 25_000_000, "ADNT_MIN": 5000, "MTBT_MAX_SEC": 30},
    2: {"ADDV_MIN": 5_000_000, "ADNT_MIN": 1000, "MTBT_MAX_SEC": 300},
}

# Define the column mapping based on the raw Polygon schema
TIMESTAMP_COL = "sip_timestamp"
PRICE_COL = "price"
SIZE_COL = "size"


def discover_tickers(base_path):
    """Discovers all tickers and their 'tick' fidelity Parquet files efficiently."""
    trading_history_root = os.path.join(base_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        logging.error(f"Trading history directory not found at: {trading_history_root}")
        return {}

    tickers_files = collections.defaultdict(list)
    logging.info(f"Scanning for 'tick' data in: {trading_history_root}")

    # Use os.scandir for efficient directory traversal
    try:
        with os.scandir(trading_history_root) as entries:
            for entry in entries:
                if entry.is_dir():
                    ticker_folder = entry.name
                    tick_path = os.path.join(entry.path, "tick")
                    if os.path.isdir(tick_path):
                        # Scan the tick_path directory
                        try:
                            with os.scandir(tick_path) as tick_entries:
                                for tick_entry in tick_entries:
                                    if tick_entry.is_file() and tick_entry.name.endswith(".parquet"):
                                        tickers_files[ticker_folder].append(tick_entry.path)
                        except Exception as e:
                            logging.warning(f"Could not scan directory {tick_path}: {e}")

    except Exception as e:
        logging.error(f"Error during directory scan of {trading_history_root}: {e}")
        return {}

    logging.info(f"Discovered {len(tickers_files)} tickers with tick data.")
    return tickers_files


def classify_liquidity(metrics):
    """Applies the tiered criteria to the calculated metrics."""
    addv = metrics.get("ADDV")
    adnt = metrics.get("ADNT")
    mtbt = metrics.get("Median_Time_Between_Trades_Sec")

    # Handle cases where metrics could not be calculated or are zero
    if None in (addv, adnt, mtbt) or addv == 0 or adnt == 0:
        return 99, "Error/Insufficient Data"

    # Check Tier 1
    if addv >= TIERS[1]["ADDV_MIN"] and adnt >= TIERS[1]["ADNT_MIN"] and mtbt <= TIERS[1]["MTBT_MAX_SEC"]:
        return 1, "Keep (High)"

    # Check Tier 2
    if addv >= TIERS[2]["ADDV_MIN"] and adnt >= TIERS[2]["ADNT_MIN"] and mtbt <= TIERS[2]["MTBT_MAX_SEC"]:
        return 2, "Keep (Moderate)"

    # Default to Tier 3
    return 3, "Remove (Illiquid)"


def calculate_liquidity_metrics(ticker, parquet_files):
    """
    Loads Parquet files using Polars Lazy API (Streaming) and calculates liquidity metrics.
    """
    # 1. Define the time cutoff
    # We use the current date/time for calculation, consistent with the previous implementation
    cutoff_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).date()

    # Optimization: Convert the cutoff date to a nanosecond timestamp integer.
    try:
        # Using pandas Timestamp for robust conversion to nanoseconds since epoch
        cutoff_timestamp_ns = int(pd.Timestamp(cutoff_date).timestamp() * 1e9)
    except Exception as e:
        return {"ticker": ticker, "error": f"Date conversion error: {e}"}

    try:
        # 2. Initialize Polars LazyFrame (Scan Parquet)
        # Filter by the integer timestamp and select necessary columns.
        df_lazy = pl.scan_parquet(parquet_files).filter(
            pl.col(TIMESTAMP_COL) >= cutoff_timestamp_ns
        ).select([TIMESTAMP_COL, PRICE_COL, SIZE_COL])

        # 3. Convert Timestamp for Analysis
        df_lazy = df_lazy.with_columns(
            # Cast to datetime (ns precision).
            pl.col(TIMESTAMP_COL).cast(pl.Datetime(time_unit="ns")).alias("datetime_ts")
        )

        # 4. Calculate Daily Statistics (Volume, Frequency, and Velocity)
        daily_stats_lazy = (
            df_lazy
            .sort("datetime_ts")
            # Group by the date component
            .group_by(pl.col("datetime_ts").dt.date().alias("date"))
            .agg(
                pl.count().alias("daily_trades"),
                # Calculate dollar volume
                (pl.col(PRICE_COL) * pl.col(SIZE_COL)).sum().alias("daily_dollar_volume"),
                # MTBT Calculation: Calculate median time differences between consecutive trades
                pl.col("datetime_ts").diff().dt.total_seconds().median().alias("daily_mtbt_sec")
            )
        )

        # Execute the query. Using streaming=True helps Polars manage memory for large datasets.
        daily_stats = daily_stats_lazy.collect(streaming=True)

        if daily_stats.is_empty():
            return {"ticker": ticker, "error": "No data in lookback period"}

        # 5. Calculate Overall Metrics
        metrics = {
            "ticker": ticker,
            "ADDV": daily_stats["daily_dollar_volume"].mean(),
            "ADNT": daily_stats["daily_trades"].mean(),
            "Median_Time_Between_Trades_Sec": daily_stats["daily_mtbt_sec"].median()
        }

        # 6. Classify
        tier, recommendation = classify_liquidity(metrics)
        metrics["Liquidity_Tier"] = tier
        metrics["Recommendation"] = recommendation

        return metrics

    except Exception as e:
        # Catch Polars errors and potential crashes (like OOM or Rust panics)
        error_message = str(e)
        # Check for the specific Rust panic/OOM indicators seen in the logs
        if "PanicException" in error_message or "Out of memory" in error_message or "could not spawn threads" in error_message or "paging file" in error_message:
            error_message = f"System Resource Error (OOM/Panic): {error_message[:150]}..."

        return {"ticker": ticker, "error": error_message}


def run_liquidity_screening():
    """Main function to orchestrate the screening process using batching."""
    logging.info("--- LAUNCHING LIQUIDITY SCREENING WORKFLOW ---")
    start_time = time.time()

    # Use the centralized file_manager to get the configuration
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        logging.error("Base output path not found in configuration.")
        return

    # 1. Discovery
    tickers_files_dict = discover_tickers(base_output_path)
    if not tickers_files_dict:
        return

    # Convert dict to list of tuples for easier batch slicing
    tickers_files_list = list(tickers_files_dict.items())
    total_tickers = len(tickers_files_list)
    all_metrics = []

    logging.info(f"\n--- Starting Calculation (Total Tickers: {total_tickers}) ---")

    # Initialize the progress bar
    pbar = tqdm(total=total_tickers, desc="Analyzing Tickers")

    # 2. Batch Processing
    # Iterate over the list in steps of BATCH_SIZE
    for i in range(0, total_tickers, BATCH_SIZE):
        batch = tickers_files_list[i:i + BATCH_SIZE]

        # Use ProcessPoolExecutor within the loop (context manager).
        # Creating and shutting down the pool per batch helps ensure resources (memory/processes) are released.
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_ticker = {}

            # Submit tasks for the current batch
            for ticker, files in batch:
                future = executor.submit(calculate_liquidity_metrics, ticker, files)
                future_to_ticker[future] = ticker

            # Process results for the current batch as they complete
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    result = future.result()
                    all_metrics.append(result)

                    # Check if the result indicates a resource error
                    if 'error' in result and "System Resource Error" in result.get('error', ''):
                        logging.warning(
                            f"Resource error processing {ticker}. Consider reducing MAX_WORKERS or BATCH_SIZE further.")

                except concurrent.futures.process.BrokenProcessPool:
                    # This occurs if a worker process is terminated abruptly (e.g., killed by OS due to OOM)
                    logging.error(
                        f"Process pool broke while processing batch near {ticker}. This indicates a severe system resource issue. Aborting.")
                    pbar.update(pbar.total - pbar.n)  # Mark remaining as done/failed
                    pbar.close()
                    logging.error("Workflow aborted due to BrokenProcessPool.")
                    return
                except Exception as e:
                    # Catch other unexpected errors during future retrieval (e.g., unpickleable exceptions like the pyo3 panics)
                    error_msg = str(e)
                    if "Can't pickle" in error_msg:
                        error_msg = "Process crashed unexpectedly (likely resource exhaustion/Rust panic)."
                    logging.error(f"Error retrieving result for {ticker}: {error_msg}")
                    all_metrics.append({"ticker": ticker, "error": f"Execution Error: {error_msg}"})

                pbar.update(1)

    pbar.close()

    # 3. Reporting
    logging.info("\n--- Generating Report ---")
    df_metrics = pd.DataFrame(all_metrics)

    # Initialize columns if they don't exist due to widespread errors
    if 'Liquidity_Tier' not in df_metrics.columns:
        df_metrics['Liquidity_Tier'] = 99
    if 'ADDV' not in df_metrics.columns:
        df_metrics['ADDV'] = 0

    # Ensure columns are numeric for sorting and handle potential NaNs
    df_metrics['Liquidity_Tier'] = pd.to_numeric(df_metrics['Liquidity_Tier'], errors='coerce').fillna(99).astype(int)
    df_metrics['ADDV'] = pd.to_numeric(df_metrics['ADDV'], errors='coerce').fillna(0)

    # Format ADDV and MTBT for readability
    df_metrics['ADDV'] = df_metrics['ADDV'].round(2)
    if 'Median_Time_Between_Trades_Sec' in df_metrics.columns:
        df_metrics['Median_Time_Between_Trades_Sec'] = pd.to_numeric(df_metrics['Median_Time_Between_Trades_Sec'],
                                                                     errors='coerce').round(2)

    df_metrics = df_metrics.sort_values(by=["Liquidity_Tier", "ADDV"], ascending=[True, False])

    # Define output path
    output_dir = os.path.join(base_output_path, "stocks", "maintenance")
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "liquidity_screening_report.csv")

    # Use file manager to get a unique path if the file exists
    try:
        unique_report_path = file_manager.get_unique_filepath(report_path)
    except AttributeError:
        # Fallback if the helper function doesn't exist in the user's file_manager.py
        logging.warning("file_manager.get_unique_filepath not found. Attempting to use default path.")
        unique_report_path = report_path

    # Save the report
    try:
        df_metrics.to_csv(unique_report_path, index=False)
        logging.info(f"Successfully saved liquidity report to: {unique_report_path}")
    except Exception as e:
        logging.error(f"Failed to save report to {unique_report_path}: {e}")

    # Summary statistics
    if 'Recommendation' in df_metrics.columns:
        summary = df_metrics['Recommendation'].value_counts()
        logging.info("\n--- Summary Statistics ---")
        print(summary)

    end_time = time.time()
    logging.info(
        f"\n--- LIQUIDITY SCREENING WORKFLOW FINISHED (Total Time: {(end_time - start_time) / 60:.2f} minutes) ---")


if __name__ == "__main__":
    # Configure Polars (optional performance tuning)
    try:
        # Setting a larger chunk size can help when using the streaming engine
        pl.Config.set_streaming_chunk_size(1_000_000)
    except Exception:
        pass

    run_liquidity_screening()