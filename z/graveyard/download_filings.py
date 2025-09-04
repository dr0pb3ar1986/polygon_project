# In download_filings.py

import os
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import re  # Import re for regex operations
from project_core import file_manager, error_logger
from z.graveyard import filing_parser

# It's good practice to define a clear, descriptive User-Agent
HEADERS = {
    'User-Agent': 'YourCompanyName contact@yourcompany.com'
}


def download_and_preprocess_html(filing_url):
    """
    Downloads the filing and performs comprehensive HTML cleanup, including iXBRL handling.
    """
    try:
        response = requests.get(filing_url, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        # It's better to log the error and return None to handle it upstream
        print(f"  > Failed to download {filing_url}: {e}")
        return None

    # Use 'lxml' for its robustness in handling messy HTML
    soup = BeautifulSoup(response.content, 'lxml')

    # 1. Remove irrelevant tags
    for element in soup(["script", "style", "noscript", "meta", "img", "head", "title"]):
        element.decompose()

    # 2. Handle iXBRL (CRITICAL for modern filings)
    # iXBRL tags (ix:) often wrap headers. We must *unwrap* them to keep the text content.
    for xbrl_tag in soup.find_all(re.compile(r"^ix:")):
        xbrl_tag.unwrap()

    # Remove specific XBRL definition tags that contain no display data
    for element in soup.find_all(['xbrli:context', 'xbrli:unit', 'xbrl', 'XBRL']):
        element.decompose()

    # 3. Remove hidden elements
    for hidden in soup.find_all(style=re.compile(r'display:\s*none|visibility:\s*hidden', re.I)):
        hidden.decompose()

    # 4. Handle Tables (Aggressive Removal for NLP)
    for table in soup.find_all("table"):
        table.decompose()

    # 5. Normalize Formatting Tags
    # Headers may be inside <b>, <i>, <font> tags. Unwrap these to make text contiguous.
    for tag in soup.find_all(['b', 'strong', 'i', 'em', 'u', 'font']):
        tag.unwrap()

    # 6. Normalize non-breaking spaces (\xa0) which break regex patterns.
    for element in soup.find_all(text=True):
        if '\xa0' in element:
            element.replace_with(element.replace('\xa0', ' '))

    return soup


def download_and_save_filing(job_details):
    ticker = job_details['ticker']
    form_type = job_details['form_type']
    filing_date = job_details['filing_date']
    filing_timestamp = job_details['filing_timestamp']
    download_url = job_details['download_url']
    target_path = job_details['target_path']

    # --- Change file extension to .jsonl ---
    target_path = os.path.splitext(target_path)[0] + ".jsonl"
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    print(f"  > Processing for {ticker}: {os.path.basename(target_path)}")
    time.sleep(0.1)  # A small delay to be respectful to the server

    # 1. Download and preprocess the HTML (now using the enhanced function)
    soup = download_and_preprocess_html(download_url)
    if not soup:
        error_message = "Download failed or returned empty content."
        error_logger.log_error(ticker, form_type, filing_date, download_url, error_message, os.path.basename(__file__))
        return

    # 2. Extract structured and cleaned content using the revised parser
    metadata = {
        'ticker': ticker,
        'form_type': form_type,
        'filing_date': filing_date,
        'filing_timestamp': filing_timestamp
    }
    # This will now use the new, more robust segmentation logic
    jsonl_content = filing_parser.segment_and_process_filing(soup, metadata)

    # 3. Save the structured JSONL output
    if jsonl_content:
        try:
            with open(target_path, "w", encoding='utf-8') as f:
                f.write(jsonl_content)
        except Exception as e:
            error_logger.log_error(ticker, form_type, filing_date, download_url, str(e), os.path.basename(__file__))
    else:
        print(f"  > No content extracted for {ticker}, skipping file creation.")


def main():
    base_output_path = file_manager.get_output_path_from_config()
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")

    if not os.path.exists(download_list_path):
        return

    try:
        df = pd.read_csv(download_list_path)
        if df.empty:
            return
        jobs = df.to_dict('records')
    except Exception:
        return

    for job in jobs:
        download_and_save_filing(job)