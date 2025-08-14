import os

from project_core import api_handler, data_processor, workflow_helpers


def _save_fundamental_data(data, base_path, fundamental_name, ticker, start_date, end_date):
    """Handles saving fundamental data to a CSV file."""
    if not data:
        print(f"  > No {fundamental_name.upper()} data returned for {ticker}, skipping save.")
        return

    output_dir = os.path.join(base_path, "stocks", "fundamentals", fundamental_name, ticker)
    filename_base = f"{ticker}_{fundamental_name}_{start_date}_to_{end_date}"
    main_filepath = os.path.join(output_dir, f"{filename_base}.csv")

    print(f"  > Saving {len(data)} {fundamental_name.upper()} records to {main_filepath}")
    data_processor.save_to_csv(data, main_filepath)


def _process_fundamentals_job(job, base_path, start_date, end_date):
    """The specific processing logic for a single fundamentals job."""
    ticker = job.get('ticker')

    fundamentals_to_fetch = {
        'financials': api_handler.get_financials_data,
        'short_interest': api_handler.get_short_interest_data,
        'short_volume': api_handler.get_short_volume_data,
    }

    for name, api_func in fundamentals_to_fetch.items():
        print(f"  > Fetching {name.upper()} for {ticker}...")
        data = api_func(ticker, start_date, end_date)
        _save_fundamental_data(data, base_path, name, ticker, start_date, end_date)


def fetch_and_save_fundamentals():
    """Main workflow to fetch fundamentals based on the target list."""
    workflow_helpers.run_target_based_workflow("Fetch Fundamentals", _process_fundamentals_job)