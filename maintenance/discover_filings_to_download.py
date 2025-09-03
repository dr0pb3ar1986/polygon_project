import os
import pandas as pd
import requests
import time
from project_core import file_manager, error_logger

# --- Constants ---
API_TOKEN = "0fb6835aea25cf2feac6fcc250e52ee5d549c37c97fcbc3871a45c6aa7ae4957"
API_URL = "https://api.sec-api.io"
HEADERS = {'User-Agent': "stirling.sloth@gmail.com"}


def get_filings_for_cik(cik):
    """Fetches all filings for a given CIK from sec-api.io."""
    # time.sleep(1.1)
    try:
        params = {
            "token": API_TOKEN, "cik": str(cik).zfill(10),
            "size": "10000", "sort": "filedAt,desc"
        }
        response = requests.get(API_URL, params=params, headers=HEADERS)

        # DEBUG: Print the exact URL being requested
        print(f"  > Requesting URL: {response.url}")

        response.raise_for_status()
        return response.json().get('filings', [])
    except requests.exceptions.RequestException as e:
        # FIXED: Added the missing 'script_name' argument to the log_error call
        script_name = os.path.basename(__file__)
        print(f"  > Error during SEC API request for {cik}: {e}")
        error_logger.log_error(str(cik), "N/A", "N/A", API_URL, f"API request failed: {e}", script_name)
        return []


def main():
    """Main function to discover missing filings and create a download list."""
    print("--- 🚀 LAUNCHING FILING DISCOVERY SCRIPT ---")
    base_output_path = file_manager.get_output_path_from_config()
    targets_path = os.path.join(base_output_path, "stocks", "stocks_filings_targets.csv")
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")

    try:
        targets_df = pd.read_csv(targets_path)
    except FileNotFoundError:
        print(f"  > Error: Target tickers file not found at {targets_path}")
        return

    print(f"Discovering missing filings for {len(targets_df)} tickers...")
    all_filings_to_download = []

    for _, row in targets_df.iterrows():
        ticker, cik = row['ticker'], row['CIK']
        print(f"-- Discovering filings for: {ticker}")

        filings = get_filings_for_cik(cik)

        for filing in filings:
            form_type = filing.get('formType', '').replace('/', '_')
            filing_date = filing.get('filedAt', '').split('T')[0]
            download_url = filing.get('linkToTxt')

            if not all([form_type, filing_date, download_url]):
                continue

            folder_name = f"{ticker}_{form_type}"
            file_name = f"{ticker}_{form_type}_{filing_date}.txt"
            target_dir = os.path.join(base_output_path, "stocks", "filings", folder_name)
            target_path = os.path.join(target_dir, file_name)

            if not os.path.exists(target_path):
                all_filings_to_download.append({
                    'ticker': ticker,
                    'form_type': form_type,
                    'filing_date': filing_date,
                    'download_url': download_url,
                    'target_path': target_path
                })

    print(f"\n--- Discovered {len(all_filings_to_download)} total filings to download. Creating target list... ---")

    if all_filings_to_download:
        try:
            pd.DataFrame(all_filings_to_download).to_csv(download_list_path, index=False)
            print(f"✅ Successfully saved download list to: {download_list_path}")
        except Exception as e:
            print(f"  > ❌ Could not save download list: {e}")

    print("\n--- ✅ DISCOVERY SCRIPT FINISHED ---")