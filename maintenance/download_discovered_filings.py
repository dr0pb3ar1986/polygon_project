# maintenance/download_discovered_filings.py

import os
import time
import pandas as pd
import concurrent.futures
from project_core import api_handler, file_manager, error_logger


def download_and_save_filing(job):
    """
    Worker function to download, parse, and save a single filing.
    """
    url = job.get('filing_url')
    target_path = job.get('target_path')
    ticker = job.get('ticker')

    if not all([url, target_path, ticker]):
        return

    try:
        print(f"  > Downloading for {ticker}: {os.path.basename(target_path)}")

        # Download, parse, and save
        raw_content = api_handler.download_raw_sec_filing(url)
        if not raw_content:
            raise ValueError("Download failed, content is empty.")

        filing_content = api_handler.parse_and_clean_filing_text(raw_content)
        if not filing_content:
            raise ValueError("Parsing failed, content is empty.")

        # Ensure directory exists before writing
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, 'w', encoding='utf-8') as f:
            f.write(filing_content)

    except Exception as e:
        print(f"  > ❌ ERROR for {ticker} - {os.path.basename(target_path)}: {e}")
        error_logger.log_error(
            ticker=ticker,
            fidelity=job.get('filing_type'),
            start_date=None,
            end_date=None,
            reason=f"Failed to download/save from {url}: {e}",
            script_name=os.path.basename(__file__)
        )


def main():
    """Main workflow to download all filings from the generated list."""
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING HIGH-SPEED FILING DOWNLOADER ---")

    base_output_path = file_manager.get_output_path_from_config()
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")

    if not os.path.exists(download_list_path):
        print("Error: Download list not found. Please run 'discover_filings_to_download.py' first.")
        return

    try:
        df = pd.read_csv(download_list_path)
        download_jobs = df.to_dict('records')
    except Exception as e:
        print(f"Error reading download list: {e}")
        return

    if not download_jobs:
        print("Download list is empty. No filings to download.")
        return

    print(f"Starting download of {len(download_jobs)} filings...")

    # High concurrency for fast I/O-bound downloading
    max_workers = 75
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(download_and_save_filing, download_jobs)

    print("\n--- ✅ DOWNLOAD SCRIPT FINISHED ---")


if __name__ == "__main__":
    main()