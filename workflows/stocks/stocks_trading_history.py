import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from project_core import api_handler, data_processor, workflow_helpers

# Define constants at the module level for clarity and easy modification.
DEBUG_SAMPLE_ROWS = 1000


def _save_trading_history_data(data, base_path, ticker, fidelity, timespan, start_date, end_date):
    """Handles the logic for saving trading history data to Parquet/CSV and creating debug files."""
    if not data:
        print(f"  > No data returned for {ticker} for range {start_date} to {end_date}, skipping save.")
        return

    fidelity_folder = fidelity.replace(" ", "-")
    output_dir = os.path.join(base_path, "stocks", "trading_history", ticker, fidelity_folder)
    filename_base = f"{ticker}_{fidelity_folder}_{start_date}_to_{end_date}"

    if timespan == "day":
        main_filepath = os.path.join(output_dir, f"{filename_base}.csv")
        print(f"  > Saving {len(data)} records to {main_filepath}")
        data_processor.save_to_csv(data, main_filepath)
    else:
        main_filepath = os.path.join(output_dir, f"{filename_base}.parquet")
        print(f"  > Saving {len(data)} records to {main_filepath}")
        data_processor.save_to_parquet(data, main_filepath)

        debug_csv_path = os.path.join(output_dir, f"{filename_base}_DEBUG.csv")
        print(f"  > Saving debug sample of {DEBUG_SAMPLE_ROWS} rows to {debug_csv_path}")
        data_processor.save_to_csv(data[:DEBUG_SAMPLE_ROWS], debug_csv_path)


def _process_trading_history_job(job, base_path, start_date, end_date):
    """
    Processes a single trading history job by breaking it into monthly chunks
    to avoid memory issues with large date ranges.
    """
    ticker = str(job.get('ticker'))
    fidelity = str(job.get('ticker_fidelity', ''))

    try:
        multiplier, timespan = workflow_helpers.parse_historical_fidelity(fidelity)
        if not timespan:
            print(f"  > Could not parse 'ticker_fidelity': '{fidelity}'. Skipping.")
            return

        overall_start_date = datetime.strptime(start_date, '%Y-%m-%d')
        overall_end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Loop through the total date range in monthly intervals
        current_date = overall_start_date
        while current_date <= overall_end_date:
            # Define the start and end of the calendar month for the current chunk
            chunk_start = current_date.replace(day=1)
            chunk_end = chunk_start + relativedelta(months=1) - relativedelta(days=1)

            # Clamp the chunk dates to the overall requested range to handle partial start/end months
            if chunk_start < overall_start_date:
                chunk_start = overall_start_date
            if chunk_end > overall_end_date:
                chunk_end = overall_end_date

            chunk_start_str = chunk_start.strftime('%Y-%m-%d')
            chunk_end_str = chunk_end.strftime('%Y-%m-%d')

            print(f"  > Processing chunk for {ticker} from {chunk_start_str} to {chunk_end_str}...")

            # Fetch and save data for this specific chunk
            if timespan == 'tick':
                fetched_data = api_handler.get_trades_data(ticker, chunk_start_str, chunk_end_str)
            else:
                fetched_data = api_handler.get_aggregate_data(ticker, multiplier, timespan, chunk_start_str, chunk_end_str)

            _save_trading_history_data(fetched_data, base_path, ticker, fidelity, timespan, chunk_start_str, chunk_end_str)

            # Move to the next month for the next iteration
            current_date = (current_date.replace(day=1) + relativedelta(months=1))

    except Exception as e:
        print(f"  > ❌ An unexpected error occurred while processing job for {ticker}: {e}")


def fetch_and_save_trading_history():
    """Main workflow to read a target list and fetch stock trading history."""
    workflow_helpers.run_target_based_workflow("Fetch Stock Trading History", _process_trading_history_job)


# This allows the file to be run directly for testing or ad-hoc execution.
if __name__ == "__main__":
    fetch_and_save_trading_history()