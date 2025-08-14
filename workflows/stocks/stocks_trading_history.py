import os

from project_core import api_handler, data_processor, workflow_helpers

# Define constants at the module level for clarity and easy modification.
DEBUG_SAMPLE_ROWS = 1000


def _save_trading_history_data(data, base_path, ticker, fidelity, timespan, start_date, end_date):
    """Handles the logic for saving trading history data to Parquet/CSV and creating debug files."""
    if not data:
        print(f"  > No data returned for {ticker}, skipping save.")
        return

    # Create a structured output path, e.g., Y:\...\stocks\trading_history\AAPL\1-minute\
    fidelity_folder = fidelity.replace(" ", "-")
    output_dir = os.path.join(base_path, "stocks", "trading_history", ticker, fidelity_folder)

    # Create a descriptive filename, e.g., AAPL_1-minute_2023-01-01_to_2023-02-01
    filename_base = f"{ticker}_{fidelity_folder}_{start_date}_to_{end_date}"

    # Determine main output format based on the parsed timespan for clarity and robustness.
    if timespan == "day":
        # For daily data, CSV is often preferred for its simplicity and universal compatibility.
        main_filepath = os.path.join(output_dir, f"{filename_base}.csv")
        print(f"  > Saving {len(data)} records to {main_filepath}")
        data_processor.save_to_csv(data, main_filepath)
    else:
        # For high-frequency intraday data (minutes, ticks), Parquet is used for its
        # superior performance and storage efficiency.
        main_filepath = os.path.join(output_dir, f"{filename_base}.parquet")
        print(f"  > Saving {len(data)} records to {main_filepath}")
        data_processor.save_to_parquet(data, main_filepath)

        # Also save a debug CSV for non-daily (parquet) files
        debug_csv_path = os.path.join(output_dir, f"{filename_base}_DEBUG.csv")
        print(f"  > Saving debug sample of {DEBUG_SAMPLE_ROWS} rows to {debug_csv_path}")
        data_processor.save_to_csv(data[:DEBUG_SAMPLE_ROWS], debug_csv_path)


def _process_trading_history_job(job, base_path, start_date, end_date):
    """
    The specific processing logic for a single trading history data job. This function
    includes error handling to ensure one failed job does not stop the entire workflow.
    """
    # Ensure ticker is explicitly a string to satisfy type checkers and prevent errors with os.path.join.
    ticker = str(job.get('ticker'))
    fidelity = str(job.get('ticker_fidelity', ''))

    try:
        multiplier, timespan = workflow_helpers.parse_historical_fidelity(fidelity)
        if not timespan:
            print(f"  > Could not parse 'ticker_fidelity': '{fidelity}'. Skipping.")
            return

        print(f"  > Fetching data for {ticker}...")
        fetched_data = None
        if timespan == 'tick':
            fetched_data = api_handler.get_trades_data(ticker, start_date, end_date)
        else:
            fetched_data = api_handler.get_aggregate_data(ticker, multiplier, timespan, start_date, end_date)

        # Pass the parsed `timespan` to the save function to avoid re-evaluating the fidelity string.
        _save_trading_history_data(fetched_data, base_path, ticker, fidelity, timespan, start_date, end_date)
    except Exception as e:
        # Catch any unexpected errors during the processing of a single job.
        # This prevents the entire workflow from crashing due to one bad ticker or API response.
        print(f"  > ❌ An unexpected error occurred while processing job for {ticker}: {e}")


def fetch_and_save_trading_history():
    """Main workflow to read a target list and fetch stock trading history."""
    workflow_helpers.run_target_based_workflow("Fetch Stock Trading History", _process_trading_history_job)