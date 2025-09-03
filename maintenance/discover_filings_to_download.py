# maintenance/discover_filings_to_download.py

import os
import time
import pandas as pd
import concurrent.futures
import re  # <-- THIS LINE WAS MISSING
from datetime import datetime
from project_core import api_handler, file_manager, error_logger

def _load_filing_targets():
    """Loads the CIK targets from the 'stocks_filings_targets.csv' file."""
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        return []
    target_file = os.path.join(base_output_path, "stocks", "stocks_filings_targets.csv")
    if not os.path.exists(target_file):
        print(f"Error: Target file not found at: {target_file}")
        return []
    try:
        df = pd.read_csv(target_file)
        df.dropna(subset=['CIK'], inplace=True)
        df = df[df['CIK'] != 'NOT_FOUND']
        return df.to_dict('records')
    except Exception as e:
        print(f"Error reading target file: {e}")
        return []

def _get_earliest_trading_date(ticker, base_path):
    """Finds the earliest start date from the trading history files for a given ticker."""
    trading_history_root = os.path.join(base_path, "stocks", "trading_history", ticker)
    if not os.path.isdir(trading_history_root):
        return None
    earliest_date = None
    # Simplified regex for this purpose
    file_pattern = re.compile(r".*_(\d{4}-\d{2}-\d{2})_to_")
    for root, _, files in os.walk(trading_history_root):
        for filename in files:
            match = file_pattern.match(filename)
            if match:
                start_date_str = match.group(1)
                current_start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                if earliest_date is None or current_start_date < earliest_date:
                    earliest_date = current_start_date
    return earliest_date.strftime('%Y-%m-%d') if earliest_date else None


def discover_missing_filings_for_ticker(job, base_output_path, worker_index=0):
    """
    Checks for missing filings for a single ticker and returns a list of files to download.
    """
    time.sleep(worker_index * 0.07) # Stagger start, slightly slower to be safe
    ticker = job.get('ticker')
    cik = job.get('CIK')
    filings_to_download = []

    print(f"-- Discovering filings for: {ticker}")
    earliest_date = _get_earliest_trading_date(ticker, base_output_path)
    if not earliest_date:
        return []

    for filing_type in ['10-K', '10-Q', '8-K']:
        time.sleep(0.5) # Paced delay before each API call
        filings = api_handler.get_sec_filings(cik, filing_type, from_date=earliest_date)
        if not filings:
            continue

        for filing in filings:
            filing_dir = os.path.join(base_output_path, "stocks", "filings", f"{ticker}_{filing_type}")
            file_name = f"{ticker}_{filing_type}_{filing.get('filedAt').split('T')[0]}.txt"
            file_path = os.path.join(filing_dir, file_name)

            if not os.path.exists(file_path):
                download_job = {
                    'ticker': ticker,
                    'filing_type': filing_type,
                    'filing_url': filing.get('linkToTxt'),
                    'target_path': file_path
                }
                filings_to_download.append(download_job)
    return filings_to_download

def main():
    """Main workflow to discover all missing filings and create a download list."""
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING FILING DISCOVERY SCRIPT ---")

    targets = _load_filing_targets()
    if not targets:
        print("No valid CIK targets found. Exiting.")
        return

    print(f"Discovering missing filings for {len(targets)} tickers...")
    base_output_path = file_manager.get_output_path_from_config()
    all_missing_filings = []

    max_workers = 12 # A safe, lower number for the sensitive API
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {
            executor.submit(discover_missing_filings_for_ticker, job, base_output_path, i): job['ticker']
            for i, job in enumerate(targets)
        }
        for future in concurrent.futures.as_completed(future_to_ticker):
            try:
                result = future.result()
                if result:
                    all_missing_filings.extend(result)
            except Exception as e:
                print(f"  > ❌ A top-level error occurred for ticker {future_to_ticker[future]}: {e}")

    if not all_missing_filings:
        print("\n--- 🎉 No missing filings found to download. ---")
        return

    print(f"\n--- Discovered {len(all_missing_filings)} total filings to download. Creating target list... ---")
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")
    try:
        df = pd.DataFrame(all_missing_filings)
        df.to_csv(download_list_path, index=False)
        print(f"✅ Successfully saved download list to: {download_list_path}")
    except Exception as e:
        print(f"  > ❌ Could not save download list: {e}")

    print("\n--- ✅ DISCOVERY SCRIPT FINISHED ---")

if __name__ == "__main__":
    main()