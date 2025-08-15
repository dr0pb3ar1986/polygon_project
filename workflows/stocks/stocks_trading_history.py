import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from project_core import api_handler, data_processor, workflow_helpers

# Define constants at the module level for clarity and easy modification.
DEBUG_SAMPLE_ROWS = 1000


def _save_trading_history_data(data_stream, base_path, ticker, fidelity, timespan, start_date, end_date, debug_callback=None):
    """Handles the logic for saving trading history data to Parquet/CSV and creating debug files."""
    fidelity_folder = fidelity.replace(" ", "-")
    output_dir = os.path.join(base_path, "stocks", "trading_history", ticker, fidelity_folder)
    filename_base = f"{ticker}_{fidelity_folder}_{start_date}_to_{end_date}"

    # Daily data is usually small, so we can materialize the stream into a list.
    if timespan == "day":
        # The stream yields pages (lists of dicts), so we flatten it.
        all_data = [item for page in data_stream for item in page]
        if not all_data:
            print(f"  > No data returned for {ticker} for range {start_date} to {end_date}, skipping save.")
            return

        main_filepath = os.path.join(output_dir, f"{filename_base}.csv")
        print(f"  > Saving {len(all_data)} records to {main_filepath}")
        data_processor.save_to_csv(all_data, main_filepath)
    else:
        # For large Parquet files, we stream directly to disk.
        main_filepath = os.path.join(output_dir, f"{filename_base}.parquet")

        data_processor.save_stream_to_parquet(
            data_stream,
            main_filepath,
            first_chunk_callback=debug_callback
        )


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

        # Define the single, persistent debug file path for the entire job.
        fidelity_folder = fidelity.replace(" ", "-")
        output_dir_base = os.path.join(base_path, "stocks", "trading_history", ticker, fidelity_folder)
        debug_file_path = os.path.join(output_dir_base, f"{ticker}_{fidelity_folder}_DEBUG.csv")
        debug_file_exists = os.path.exists(debug_file_path)

        overall_start_date = datetime.strptime(start_date, '%Y-%m-%d')
        overall_end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Loop through the total date range in monthly intervals
        current_date = overall_start_date
        is_first_chunk = True  # Track the first iteration of the loop
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
                data_stream = api_handler.stream_trades_data(ticker, chunk_start_str, chunk_end_str)
            else:
                data_stream = api_handler.stream_aggregate_data(ticker, multiplier, timespan, chunk_start_str, chunk_end_str)

            # Prepare the debug callback only for the first chunk if the file doesn't exist.
            debug_callback = None
            if is_first_chunk and not debug_file_exists and timespan != 'day':
                def save_debug_file_once(first_chunk):
                    """A callback to save the first few rows for easy inspection."""
                    print(f"  > Saving one-time debug sample of up to {DEBUG_SAMPLE_ROWS} rows to {debug_file_path}")
                    data_processor.save_to_csv(first_chunk[:DEBUG_SAMPLE_ROWS], debug_file_path)
                debug_callback = save_debug_file_once

            _save_trading_history_data(data_stream, base_path, ticker, fidelity, timespan, chunk_start_str, chunk_end_str, debug_callback=debug_callback)

            # Move to the next month for the next iteration
            current_date = (current_date.replace(day=1) + relativedelta(months=1))
            is_first_chunk = False  # Ensure callback is only prepared once

    except Exception as e:
        print(f"  > ❌ An unexpected error occurred while processing job for {ticker}: {e}")


def fetch_and_save_trading_history():
    """Main workflow to read a target list and fetch stock trading history."""
    workflow_helpers.run_target_based_workflow("Fetch Stock Trading History", _process_trading_history_job)


# This allows the file to be run directly for testing or ad-hoc execution.
if __name__ == "__main__":
    fetch_and_save_trading_history()