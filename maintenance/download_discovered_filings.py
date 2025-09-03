import os
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from project_core import file_manager, error_logger, filing_parser

HEADERS = {
    'User-Agent': 'PolygonProject Agent stirling.sloth@gmail.com'
}

def download_and_save_filing(job_details):
    ticker = job_details['ticker']
    form_type = job_details['form_type']
    filing_date = job_details['filing_date']
    filing_timestamp = job_details['filing_timestamp'] # Get the new timestamp
    download_url = job_details['download_url']
    target_path = job_details['target_path']

    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    print(f"  > Downloading for {ticker}: {os.path.basename(target_path)}")
    time.sleep(0.15)

    try:
        response = requests.get(download_url, headers=HEADERS)
        response.raise_for_status()

        if response.content:
            soup = BeautifulSoup(response.content, 'lxml')
            raw_text = soup.get_text(separator='\\n')

            metadata = {
                'ticker': ticker,
                'form_type': form_type,
                'filing_date': filing_date,
                'filing_timestamp': filing_timestamp # Pass it to the parser
            }
            formatted_content = filing_parser.parse_to_structured_text(raw_text, metadata)

            with open(target_path, "w", encoding='utf-8') as f:
                f.write(formatted_content)
        else:
            error_message = "Download failed, content is empty."
            error_logger.log_error(ticker, form_type, filing_date, download_url, error_message, os.path.basename(__file__))

    except requests.exceptions.RequestException as e:
        error_logger.log_error(ticker, form_type, filing_date, download_url, str(e), os.path.basename(__file__))
    except Exception as e:
        error_logger.log_error(ticker, form_type, filing_date, download_url, str(e), os.path.basename(__file__))


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