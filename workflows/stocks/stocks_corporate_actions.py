import os

from project_core import api_handler, data_processor, workflow_helpers


def _save_corporate_action_data(data, base_path, action_name, ticker, start_date, end_date):
    """Handles saving corporate action data to a CSV file."""
    if not data:
        print(f"  > No {action_name.upper()} data returned for {ticker}, skipping save.")
        return

    output_dir = os.path.join(base_path, "stocks", "corporate_actions", action_name, ticker)
    filename_base = f"{ticker}_{action_name}_{start_date}_to_{end_date}"
    main_filepath = os.path.join(output_dir, f"{filename_base}.csv")

    print(f"  > Saving {len(data)} {action_name.upper()} records to {main_filepath}")
    data_processor.save_to_csv(data, main_filepath)


def _process_corporate_actions_job(job, base_path, start_date, end_date):
    """The specific processing logic for a single corporate actions job."""
    ticker = job.get('ticker')

    actions_to_fetch = {
        'ipo': api_handler.get_ipos_data,
        'splits': api_handler.get_splits_data,
        'dividends': api_handler.get_dividends_data,
        'ticker_events': api_handler.get_ticker_events_data,
    }

    for name, api_func in actions_to_fetch.items():
        print(f"  > Fetching {name.upper()} for {ticker}...")
        data = api_func(ticker, start_date, end_date)
        _save_corporate_action_data(data, base_path, name, ticker, start_date, end_date)


def fetch_and_save_corporate_actions():
    """Main workflow to fetch corporate actions based on the target list."""
    workflow_helpers.run_target_based_workflow("Fetch Corporate Actions", _process_corporate_actions_job)