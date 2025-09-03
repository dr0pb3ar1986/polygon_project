# workflows/stocks/stocks_sec_filings.py

import os
import time
import re
from datetime import datetime
from project_core import api_handler, file_manager, error_logger


def _get_tickers_with_trading_history():
    """Gets a list of tickers from the trading_history directory."""
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        return []

    trading_history_root = os.path.join(base_output_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        print(f"Error: Trading history directory not found at: {trading_history_root}")
        return []

    return [d for d in os.listdir(trading_history_root) if os.path.isdir(os.path.join(trading_history_root, d))]


def _get_earliest_trading_date(ticker, base_path):
    """Finds the earliest start date from the trading history files for a given ticker."""
    trading_history_root = os.path.join(base_path, "stocks", "trading_history", ticker)
    if not os.path.isdir(trading_history_root):
        return None

    earliest_date = None
    file_pattern = re.compile(r".*_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.(parquet|csv)")

    for fidelity_folder in os.listdir(trading_history_root):
        fidelity_path = os.path.join(trading_history_root, fidelity_folder)
        if os.path.isdir(fidelity_path):
            for filename in os.listdir(fidelity_path):
                match = file_pattern.match(filename)
                if match:
                    start_date_str = match.group(1)
                    current_start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                    if earliest_date is None or current_start_date < earliest_date:
                        earliest_date = current_start_date

    return earliest_date.strftime('%Y-%m-%d') if earliest_date else None


def _process_ticker_filings(ticker, base_output_path):
    """Fetches and saves SEC filings for a single ticker."""
    print(f"\n--- Processing Filings for Ticker: {ticker} ---")

    earliest_date = _get_earliest_trading_date(ticker, base_output_path)
    if not earliest_date:
        print(f"  > No trading history files found for {ticker}. Skipping SEC filings.")
        return

    print(f"  > Earliest trading data found: {earliest_date}. Fetching filings from this date.")

    cik = api_handler.get_cik_for_ticker(ticker)
    if not cik:
        print(f"  > Could not find CIK for {ticker}. Skipping.")
        return

    filings_output_dir = os.path.join(base_output_path, "stocks", "filings")

    for filing_type in ['10-K', '10-Q', '8-K']:
        filings = api_handler.get_sec_filings(cik, filing_type, from_date=earliest_date)
        if not filings:
            print(f"  > No {filing_type} filings found for {ticker} since {earliest_date}.")
            continue

        for filing in filings:
            try:
                # Determine file path first to check for existence
                filing_dir = os.path.join(filings_output_dir, f"{ticker}_{filing_type}")
                file_name = f"{ticker}_{filing_type}_{filing.get('filedAt').split('T')[0]}.txt"
                file_path = os.path.join(filing_dir, file_name)

                # Check if the file already exists
                if os.path.exists(file_path):
                    print(f"  > Skipping, already exists: {file_name}")
                    continue

                # If it doesn't exist, proceed with download and save
                filing_txt_url = filing.get('linkToTxt')
                if not filing_txt_url:
                    continue

                # Step 1: Download the full raw content (as bytes)
                raw_content = api_handler.download_raw_sec_filing(filing_txt_url)
                if not raw_content:
                    continue  # Skip if download failed

                # Step 2: Parse and clean the raw content to get just the body
                filing_content = api_handler.parse_and_clean_filing_text(raw_content)

                if filing_content:
                    # Create the directory structure (safe to call multiple times)
                    os.makedirs(filing_dir, exist_ok=True)

                    # Save the filing
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(filing_content)
                    print(f"  > Saved: {file_name}")

            except Exception as e:
                error_logger.log_error(ticker, filing_type, None, None, e, os.path.basename(__file__))
                print(f"  > ❌ An error occurred while processing a filing for {ticker}: {e}")
            finally:
                # A short, single delay per filing is enough to stay within rate limits.
                # The SEC allows 40 requests/sec. 0.05s is a safe delay.
                time.sleep(0.05)


def fetch_and_save_sec_filings():
    """
    Main workflow to fetch SEC filings for tickers with trading history.
    """
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING SEC FILINGS WORKFLOW ---")

    tickers = _get_tickers_with_trading_history()
    if not tickers:
        print("No tickers with trading history found. Exiting SEC filings workflow.")
        return

    print(f"Found {len(tickers)} tickers with trading history. Fetching filings...")

    base_output_path = file_manager.get_output_path_from_config()

    # Processing tickers sequentially. Concurrency can be added back later if needed.
    for ticker in tickers:
        _process_ticker_filings(ticker, base_output_path)

    print("\n--- ✅ SEC FILINGS WORKFLOW FINISHED ---")


if __name__ == "__main__":
    fetch_and_save_sec_filings()
