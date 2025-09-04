import os
import pandas as pd
import requests
import time
from project_core import file_manager, error_logger

# --- Constants ---
API_TOKEN = "0fb6835aea25cf2feac6fcc250e52ee5d549c37c97fcbc3871a45c6aa7ae4957"
API_URL = "https://api.sec-api.io"
HEADERS = {'User-Agent': "PolygonProject Agent stirling.sloth@gmail.com", 'Content-Type': 'application/json'}


def get_filings(cik, form_type):
    all_filings = []
    current_from = 0
    page_size = 50

    while True:
        try:
            formatted_cik = str(int(cik))
        except (ValueError, TypeError):
            return []

        query = {
            "query": {"query_string": {
                "query": f"cik:\"{formatted_cik}\" AND formType:\"{form_type}\""
            }},
            "from": str(current_from),
            "size": str(page_size),
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        params = {"token": API_TOKEN}

        try:
            response = requests.post(API_URL, params=params, json=query, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            filings_on_page = data.get('filings', [])

            if not filings_on_page:
                break

            all_filings.extend(filings_on_page)

            if len(filings_on_page) < page_size:
                break

            current_from += page_size
            time.sleep(0.1)

        except requests.exceptions.RequestException:
            break

    return all_filings


def main():
    print("--- 🚀 LAUNCHING FILING DISCOVERY SCRIPT ---")
    base_output_path = file_manager.get_output_path_from_config()
    targets_path = os.path.join(base_output_path, "stocks", "stocks_filings_targets.csv")
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")

    try:
        targets_df = pd.read_csv(targets_path)
    except FileNotFoundError:
        return

    print(f"Discovering missing filings for {len(targets_df)} tickers...")
    all_filings_to_download = []
    filing_types_to_query = ['10-K', '10-Q', '8-K']

    for _, row in targets_df.iterrows():
        ticker, cik = row['ticker'], row['CIK']
        if pd.isna(cik) or cik == 'NOT_FOUND':
            continue
        print(f"-- Discovering filings for: {ticker}")

        for form_type_query in filing_types_to_query:
            filings = get_filings(cik, form_type_query)

            for filing in filings:
                original_form_type = filing.get('formType', '')
                base_form_type = form_type_query

                suffix = ''
                if original_form_type.endswith('/A'):
                    suffix = '_A'
                if original_form_type.startswith('NT'):
                    suffix = '_NT'

                folder_name = f"{ticker}_{base_form_type}"
                file_name_base = f"{ticker}_{base_form_type}"

                # --- CAPTURE GRANULAR TIMESTAMP ---
                filed_at_timestamp = filing.get('filedAt', 'N/A')
                filing_date = filed_at_timestamp.split('T')[0] if 'T' in filed_at_timestamp else filed_at_timestamp
                # --- ---

                download_url = filing.get('linkToTxt')

                if not all([original_form_type, filing_date, download_url]):
                    continue

                file_name = f"{file_name_base}{suffix}_{filing_date}.txt"
                target_dir = os.path.join(base_output_path, "stocks", "Filings", folder_name)
                target_path = os.path.join(target_dir, file_name)

                if not os.path.exists(target_path):
                    all_filings_to_download.append({
                        'ticker': ticker,
                        'form_type': original_form_type,
                        'filing_date': filing_date,
                        'filing_timestamp': filed_at_timestamp,  # Add new timestamp field
                        'download_url': download_url,
                        'target_path': target_path
                    })

    if all_filings_to_download:
        pd.DataFrame(all_filings_to_download).to_csv(download_list_path, index=False)
        print(f"✅ Successfully saved download list to: {download_list_path}")

    print("\n--- ✅ DISCOVERY SCRIPT FINISHED ---")


if __name__ == "__main__":
    main()