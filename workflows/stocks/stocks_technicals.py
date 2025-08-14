import os
from project_core import api_handler, data_processor, workflow_helpers


def _save_technical_data(data, base_path, indicator_name, ticker, multiplier, timespan, start_date, end_date):
    """Handles the logic for saving technical indicator data to a CSV file."""
    if not data:
        print(f"  > No {indicator_name.upper()} data returned for {ticker}, skipping save.")
        return

    # Create a structured output path, e.g., Y:\...\stocks\technicals\sma\AAPL\1-minute\
    fidelity_folder = f"{multiplier}-{timespan}"
    output_dir = os.path.join(str(base_path), "stocks", "technicals", indicator_name, str(ticker), fidelity_folder)

    # Create a descriptive filename
    filename_base = f"{ticker}_{indicator_name}_{start_date}_to_{end_date}"
    main_filepath = os.path.join(str(output_dir), f"{filename_base}.csv")

    print(f"  > Saving {len(data)} {indicator_name.upper()} records to {main_filepath}")
    data_processor.save_to_csv(data, main_filepath)


def _process_technicals_job(job, base_path, start_date, end_date):
    """
    The specific processing logic for a single technical indicators job.
    """
    ticker = str(job.get('ticker'))
    fidelity = str(job.get('ticker_fidelity', ''))

    multiplier, timespan = workflow_helpers.parse_technicals_fidelity(fidelity)
    if not timespan:
        print(f"  > Could not parse 'ticker_fidelity': '{fidelity}'. Skipping.")
        return

    # Define the indicators to fetch and their corresponding API functions
    indicators_to_fetch = {
        'sma': api_handler.get_sma_data,
        'ema': api_handler.get_ema_data,
        'macd': api_handler.get_macd_data,
        'rsi': api_handler.get_rsi_data,
    }

    # Loop through, fetch, and save data for each indicator
    for name, api_func in indicators_to_fetch.items():
        # The API functions for indicators don't need the multiplier, just the timespan
        data = api_func(ticker, timespan, start_date, end_date)
        _save_technical_data(data, base_path, name, ticker, multiplier, timespan, start_date, end_date)


def fetch_and_save_technical_indicators():
    """
    Main workflow to read a target list and fetch technical indicators for each stock.
    """
    workflow_helpers.run_target_based_workflow("Fetch Technical Indicators", _process_technicals_job)


# This allows the file to be run directly for testing
if __name__ == "__main__":
    fetch_and_save_technical_indicators()