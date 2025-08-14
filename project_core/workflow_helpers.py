import os
from datetime import datetime
import concurrent.futures

# Import project tools
from . import api_handler, data_processor, file_manager
from dateutil.relativedelta import relativedelta


def _get_max_workers():
    """A private helper to read the number of concurrent workers from config."""
    return file_manager.get_config_value('api_settings', 'max_concurrent_requests', fallback=10, type_cast=int)


def run_paginated_list_workflow(asset_class, endpoint, params):
    """
    A generic workflow to fetch a paginated list of tickers and save it.
    """
    folder_name, filename_prefix = file_manager.get_asset_class_paths(asset_class)
    print(f"--- Starting Generic List Workflow for: {asset_class.upper()} ---")

    all_data = api_handler.get_paginated_data(endpoint, params)

    if all_data:
        base_output_path = file_manager.get_output_path_from_config()
        if base_output_path:
            today_str = datetime.now().strftime('%Y%m%d')
            desired_path = os.path.join(base_output_path, folder_name, f'{filename_prefix}-{today_str}-ticker-list.csv')
            unique_path = file_manager.get_unique_filepath(desired_path)
            data_processor.save_to_csv(all_data, unique_path)

    print("--- Workflow Finished ---")


def _save_workflow_results(data, asset_class, file_suffix):
    """A private helper to save the results of a workflow to a unique CSV file."""
    if not data:
        print("No data to save.")
        return

    print(f"Successfully processed {len(data)} records.")
    base_output_path = file_manager.get_output_path_from_config()
    if base_output_path:
        folder_name, filename_prefix = file_manager.get_asset_class_paths(asset_class)
        today_str = datetime.now().strftime('%Y%m%d')
        desired_path = os.path.join(base_output_path, folder_name, f'{filename_prefix}-{today_str}-{file_suffix}.csv')
        unique_path = file_manager.get_unique_filepath(desired_path)
        data_processor.save_to_csv(data, unique_path)


def run_concurrent_overview_workflow(asset_class, api_function, post_processor=None):
    """
    A generic workflow to fetch overviews for a list of tickers concurrently.
    """
    max_workers = _get_max_workers()
    print(f"--- Starting Generic Overview Workflow for: {asset_class.upper()} ---")
    tickers_to_process = data_processor.load_latest_ticker_list(asset_class)

    if tickers_to_process.size == 0:
        print(f"No tickers found for {asset_class}. Skipping overview fetch.")
        print("--- Workflow Finished ---")
        return

    print(f"Found {len(tickers_to_process)} unique {asset_class} tickers to process.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(api_function, tickers_to_process)
        all_overviews = [result for result in results if result is not None]

    if post_processor and all_overviews:
        print("Applying post-processing to format data...")
        all_overviews = post_processor(all_overviews)

    if all_overviews:
        _save_workflow_results(all_overviews, asset_class, "ticker-overview")
    print("--- Workflow Finished ---")


def format_stock_overview_data(overview_list):
    """
    A post-processor to clean up data from the stock ticker details endpoint.
    """
    for overview in overview_list:
        overview.pop('branding', None)
        address_data = overview.get('address')
        if isinstance(address_data, dict):
            address_parts = [
                address_data.get('address1'),
                address_data.get('city'),
                address_data.get('state'),
                address_data.get('postal_code')
            ]
            overview['address'] = ', '.join(filter(None, address_parts))
    return overview_list


def run_concurrent_relatives_workflow():
    """
    A specific workflow for fetching 'related tickers' for stocks concurrently.
    """
    print("--- Starting Workflow: Fetch All Stock Ticker Relatives ---")
    tickers_to_process = data_processor.load_latest_ticker_list('stocks')

    if tickers_to_process.size == 0:
        print("No stock tickers found. Skipping relatives fetch.")
        print("--- Workflow Finished ---")
        return

    print(f"Found {len(tickers_to_process)} unique tickers to process.")
    max_workers = _get_max_workers()
    all_relations = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(api_handler.get_related_tickers, ticker): ticker for ticker in
                            tickers_to_process}
        for future in concurrent.futures.as_completed(future_to_ticker):
            original_ticker = future_to_ticker[future]
            try:
                related_tickers = future.result()
                if related_tickers:
                    for related_dict in related_tickers:
                        if isinstance(related_dict, dict) and 'ticker' in related_dict:
                            all_relations.append({'ticker': original_ticker, 'related_ticker': related_dict['ticker']})
            except Exception as e:
                print(f"Error processing relatives for {original_ticker}: {e}")

    if all_relations:
        _save_workflow_results(all_relations, 'stocks', "ticker-relatives")
    print("--- Workflow Finished ---")


def run_target_based_workflow(workflow_name, job_processor_func):
    """
    A generic helper to run workflows based on the stock_ticker_targets.csv file.
    """
    print(f"--- Starting Workflow: {workflow_name} ---")
    target_file_path = file_manager.get_config_value('file_paths', 'stock_targets_csv')
    if not target_file_path:
        print("Error: 'stock_targets_csv' path not found in config.ini. Exiting workflow.")
        return

    targets = data_processor.load_target_tickers(target_file_path)
    if not targets:
        print("No targets found or file could not be read. Exiting workflow.")
        return

    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Base output path not configured. Exiting workflow.")
        return

    today = datetime.now()

    for job in targets:
        ticker = job.get('ticker')
        fidelity = str(job.get('ticker_fidelity', ''))
        duration_months = job.get('ticker_duration_months')

        if not all([ticker, fidelity, duration_months]):
            print(f"  > Skipping invalid job due to missing data: {job}")
            continue

        print(f"\n--- Processing Job for Ticker: {ticker}, Fidelity: {fidelity} ---")

        try:
            start_date = today - relativedelta(months=int(duration_months))
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = today.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            print(f"  > Invalid 'ticker_duration_months': {duration_months}. Skipping.")
            continue

        job_processor_func(job, base_output_path, start_date_str, end_date_str)

    print(f"\n--- ✅ {workflow_name} Workflow Finished ---")


# --- ADD THESE FUNCTIONS ---
def parse_historical_fidelity(fidelity_str):
    """
    Parses a fidelity string (e.g., '1 day', '5 minute') into a multiplier and timespan.
    Returns (None, None) if parsing fails.
    """
    if not isinstance(fidelity_str, str):
        return None, None

    parts = fidelity_str.lower().split()
    if len(parts) == 2 and parts[0].isdigit():
        return int(parts[0]), parts[1]
    return None, None


def parse_technicals_fidelity(fidelity_str):
    """
    Parses a fidelity string for technicals (e.g., '1-day', '5-minute') into a multiplier and timespan.
    """
    if not isinstance(fidelity_str, str):
        return None, None

    parts = fidelity_str.lower().split('-')
    if len(parts) == 2 and parts[0].isdigit():
        return int(parts[0]), parts[1]
    return None, None