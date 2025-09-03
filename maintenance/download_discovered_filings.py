import os
import pandas as pd
import requests
import concurrent.futures
import time
from project_core import file_manager, error_logger

# --- Constants ---
# SEC's rate limit is 10 requests/sec. We'll be conservative.
# Headers to mimic a browser visit
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

def download_and_save_filing(job_details):
    """
    Downloads a single filing based on the job details provided.
    """
    ticker = job_details['ticker']
    form_type = job_details['form_type']
    filing_date = job_details['filing_date']
    download_url = job_details['download_url']
    target_path = job_details['target_path']

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    # Friendly print statement for the user
    print(f"  > Downloading for {ticker}: {os.path.basename(target_path)}")

    # Add a delay to respect the SEC's rate limits
    time.sleep(0.15)

    try:
        response = requests.get(download_url, headers=HEADERS)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        if response.content:
            with open(target_path, "wb") as f:
                f.write(response.content)
        else:
            error_message = f"Download failed, content is empty."
            print(f"  > ❌ ERROR for {ticker} - {os.path.basename(target_path)}: {error_message}")
            error_logger.log_error(ticker, filing_date, filing_date, download_url, error_message)

    except requests.exceptions.RequestException as e:
        error_message = f"Failed to download/save from {download_url}: {e}"
        print(f"  > 🔴 ERROR LOGGED for {ticker} ({filing_date}): {error_message}")
        error_logger.log_error(ticker, filing_date, filing_date, download_url, str(e))


def main():
    """
    Main function to orchestrate the download of discovered filings.
    """
    base_output_path = file_manager.get_output_path_from_config()
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")

    if not os.path.exists(download_list_path):
        print("  > Download list not found. Please run the discovery script first.")
        return

    try:
        df = pd.read_csv(download_list_path)
        if df.empty:
            print("  > Download list is empty. No new filings to download.")
            return
        jobs = df.to_dict('records')
    except Exception as e:
        print(f"  > Error reading the download list: {e}")
        return

    # Using ThreadPoolExecutor to download files in parallel
    # Adjust max_workers based on your machine's capabilities and network.
    # Start with a conservative number and increase if stable.
    max_workers = 50
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(download_and_save_filing, jobs)


if __name__ == "__main__":
    main()