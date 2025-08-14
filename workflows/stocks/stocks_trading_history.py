import os
from project_core import api_handler, data_processor, workflow_helpers

# Define constants at the module level for clarity and easy modification.
DEBUG_SAMPLE_ROWS = 1000


def _save_trading_history_data(data, base_path, ticker, fidelity, timespan, start_date, end_date):
    """Handles the logic for saving trading history data to Parquet/CSV and creating debug files."""
    if not data:
        print(f"  > No data returned for {ticker}, skipping save.")
        return

    fidelity_folder = fidelity.replace(" ", "-")
    output_dir = os.path.join(str(base_path), "stocks", "trading_history", str(ticker), str(fidelity_folder))
    filename_base = f"{ticker}_{fidelity_folder}_{start_date}_to_{end_date}"

    if timespan == "day":
        main_filepath = os.path.join(str(output_dir), f"{filename_base}.csv")
        print(f"  > Saving {len(data)} records to {main_filepath}")
        data_processor.save_to_csv(data, main_filepath)
    else:
        main_filepath = os.path.join(str(output_dir), f"{filename_base}.parquet")
        print(f"  > Saving {len(data)} records to {main_filepath}")
        data_processor.save_to_parquet(data, main_filepath)

        debug_csv_path = os.path.join(str(output_dir), f"{filename_base}_DEBUG.csv")
        print(f"  > Saving debug sample of {DEBUG_SAMPLE_ROWS} rows to {debug_csv_path}")
        data_processor.save_to_csv(data[:DEBUG_SAMPLE_ROWS], debug_csv_path)


def _process_trading_history_job(job, base_path, start_date, end_date):
    """
    The specific processing logic for a single trading history data job.
    """
    ticker = str(job.get('ticker'))
    fidelity = str(job.get('ticker_fidelity', ''))

    try:
        multiplier, timespan = workflow_helpers.parse_historical_fidelity(fidelity)
        if not timespan:
            print(f"  > Could not parse 'ticker_fidelity': '{fidelity}'. Skipping.")
            return

        print(f"  > Fetching data for {ticker}...")

        # This structure fixes the "unused variable" warning
        if timespan == 'tick':
            fetched_data = api_handler.get_trades_data(ticker, start_date, end_date)
            _save_trading_history_data(fetched_data, base_path, ticker, fidelity, timespan, start_date, end_date)
        else:
            fetched_data = api_handler.get_aggregate_data(ticker, multiplier, timespan, start_date, end_date)
            _save_trading_history_data(fetched_data, base_path, ticker, fidelity, timespan, start_date, end_date)

    except Exception as e:
        print(f"  > ❌ An unexpected error occurred while processing job for {ticker}: {e}")


def fetch_and_save_trading_history():
    """Main workflow to read a target list and fetch stock trading history."""
    workflow_helpers.run_target_based_workflow("Fetch Stock Trading History", _process_trading_history_job)