import os
import pandas as pd
import requests
import concurrent.futures
import time
from bs4 import BeautifulSoup
from project_core import file_manager, error_logger

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}


def download_and_save_filing(job_details):
    """
    Downloads, processes with BeautifulSoup, and saves a single filing.
    """
    ticker = job_details['ticker']
    filing_date = job_details['filing_date']
    download_url = job_details['download_url']
    target_path = job_details['target_path']

    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    print(f"  > Downloading for {ticker}: {os.path.basename(target_path)}")

    time.sleep(0.15)

    try:
        response = requests.get(download_url, headers=HEADERS)
        response.raise_for_status()

        if response.content:
            # --- START: BeautifulSoup Parsing Logic ---
            soup = BeautifulSoup(response.content, 'lxml')

            # Find all <DOCUMENT> tags
            docs = soup.find_all('document')

            content_to_save = ""
            if docs:
                # Typically, we want the text from the first complete submission document
                # which is usually the first document that is not of type 'COVER' or 'GRAPHIC'.
                for doc in docs:
                    doc_type_tag = doc.find('type')
                    if doc_type_tag and doc_type_tag.get_text(strip=True).upper() not in ['GRAPHIC', 'COVER', 'EXCEL',
                                                                                          'JSON']:
                        text_tag = doc.find('text')
                        if text_tag:
                            content_to_save = text_tag.get_text(separator='\\n')
                            break  # Found the main document, exit loop

            # If no suitable <DOCUMENT> tag was found, fall back to the whole text
            if not content_to_save:
                content_to_save = soup.get_text(separator='\\n')
            # --- END: BeautifulSoup Parsing Logic ---

            with open(target_path, "w", encoding='utf-8') as f:
                f.write(content_to_save)
        else:
            error_message = "Download failed, content is empty."
            print(f"  > ❌ ERROR for {ticker} - {os.path.basename(target_path)}: {error_message}")
            error_logger.log_error(ticker, filing_date, "N/A", download_url, error_message)

    except requests.exceptions.RequestException as e:
        error_message = f"Failed to download/save from {download_url}: {e}"
        print(f"  > 🔴 ERROR LOGGED for {ticker} ({filing_date}): {error_message}")
        error_logger.log_error(ticker, filing_date, "N/A", download_url, str(e))
    except Exception as e:
        error_message = f"An unexpected error occurred processing {download_url}: {e}"
        print(f"  > 🔴 UNEXPECTED ERROR LOGGED for {ticker}: {error_message}")
        error_logger.log_error(ticker, filing_date, "N/A", download_url, str(e))


def main():
    """Main function to orchestrate the download of discovered filings."""
    base_output_path = file_manager.get_output_path_from_config()
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")

    if not os.path.exists(download_list_path):
        print("  > Download list not found. Nothing to download.")
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

    max_workers = 50
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_and_save_filing, job) for job in jobs]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"  > ❌ An error occurred in a download thread: {e}")