# workflows/stocks/stocks_sec_filings.py

import os
import time
import re
from datetime import datetime
import pandas as pd
import concurrent.futures
from project_core import api_handler, file_manager, error_logger


def _load_filing_targets():
    """
    Loads the CIK targets from the 'stocks_filings_targets.csv' file.
    """
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        return []

    target_file = os.path.join(base_output_path, "stocks", "stocks_filings_targets.csv")
    if not os.path.exists(target_file):
        print(f"Error: Target file not found at: {target_file}")
        return []

    try:
        df = pd.read_csv(target_file)
        # Drop rows where CIK is missing or marked as NOT_FOUND
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


def _process_filing_job(job, base_output_path, worker_index=0):
    """
    Fetches and saves SEC filings for a single ticker, with a staggered start.
    """
    # Stagger the start of each worker to prevent initial burst requests.
    # 15 workers * 0.06s = 0.9s total stagger time.
    time.sleep(worker_index * 0.06)

    ticker = job.get('ticker')
    cik = job.get('CIK')

    print(f"\n--- Processing Filings for Ticker: {ticker} (CIK: {cik}) ---")

    earliest_date = _get_earliest_trading_date(ticker, base_output_path)
    if not earliest_date:
        print(f"  > No trading history files found for {ticker}. Skipping SEC filings.")
        return

    print(f"  > Earliest trading data found: {earliest_date}. Fetching filings from this date.")

    filings_output_dir = os.path.join(base_output_path, "stocks", "filings")

    for filing_type in ['10-K', '10-Q', '8-K']:
        filings = api_handler.get_sec_filings(cik, filing_type, from_date=earliest_date)
        if not filings:
            print(f"  > No {filing_type} filings found for {ticker} since {earliest_date}.")
            continue

        for filing in filings:
            try:
                filing_dir = os.path.join(filings_output_dir, f"{ticker}_{filing_type}")
                file_name = f"{ticker}_{filing_type}_{filing.get('filedAt').split('T')[0]}.txt"
                file_path = os.path.join(filing_dir, file_name)

                if os.path.exists(file_path):
                    print(f"  > Skipping, already exists: {file_name}")
                    # Add a tiny delay when skipping to pace the workers
                    time.sleep(0.05)
                    continue

                filing_txt_url = filing.get('linkToTxt')
                if not filing_txt_url:
                    continue

                raw_content = api_handler.download_raw_sec_filing(filing_txt_url)
                if not raw_content:
                    continue

                filing_content = api_handler.parse_and_clean_filing_text(raw_content)

                if filing_content:
                    os.makedirs(filing_dir, exist_ok=True)
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(filing_content)
                    print(f"  > Saved: {file_name}")

            except Exception as e:
                error_logger.log_error(ticker, filing_type, None, None, e, os.path.basename(__file__))
                print(f"  > ❌ An error occurred while processing a filing for {ticker}: {e}")
            finally:
                # This delay paces the sustained filing downloads.
                time.sleep(0.4)


def fetch_and_save_sec_filings():
    """
    Main workflow to fetch SEC filings based on the pre-generated target list.
    """
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING SEC FILINGS WORKFLOW (STAGGERED START) ---")

    targets = _load_filing_targets()
    if not targets:
        print("No valid targets found in 'stocks_filings_targets.csv'. Exiting.")
        return

    print(f"Found {len(targets)} tickers with valid CIKs to process.")

    base_output_path = file_manager.get_output_path_from_config()

    max_workers = 15
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # We use enumerate to get an index for each job to use for staggering.
        futures = {
            executor.submit(_process_filing_job, job, base_output_path, worker_index=i): job['ticker']
            for i, job in enumerate(targets)
        }

        for future in concurrent.futures.as_completed(futures):
            ticker = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"  > ❌ A top-level error occurred while processing ticker {ticker}: {e}")

    print("\n--- ✅ SEC FILINGS WORKFLOW FINISHED ---")


if __name__ == "__main__":
    fetch_and_save_sec_filings()