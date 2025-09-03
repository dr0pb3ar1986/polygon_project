# maintenance/create_filings_target_list.py

import os
import time
import pandas as pd
import concurrent.futures

# Import project tools
from project_core import api_handler, file_manager

# --- USER CONFIGURABLE SETTINGS ---
# Adjust the sleep time (in seconds) between each API call.
# 1 / 40 requests per second = 0.025s. A value slightly higher is safer to avoid rate limiting.
SLEEP_BETWEEN_REQUESTS = 0.2
# ------------------------------------


def get_tickers_from_history():
    """
    Scans the trading_history directory to get a list of all unique tickers.
    """
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Error: 'base_output_path' not found in config.ini. Exiting.")
        return []

    trading_history_root = os.path.join(base_output_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        print(f"Error: Trading history directory not found at: {trading_history_root}")
        return []

    # The tickers are the names of the folders inside the trading_history directory
    tickers = [d for d in os.listdir(trading_history_root) if
               os.path.isdir(os.path.join(trading_history_root, d))]
    print(f"Found {len(tickers)} unique tickers in the trading history folder.")
    return tickers


def fetch_cik_for_ticker(ticker):
    """
    Worker function to fetch the CIK for a single ticker and handle sleep.
    """
    print(f"  > Fetching CIK for: {ticker}")
    cik = api_handler.get_cik_for_ticker(ticker)
    time.sleep(SLEEP_BETWEEN_REQUESTS)  # Pause to respect rate limits
    return {'ticker': ticker, 'CIK': cik if cik else 'NOT_FOUND'}


def main():
    """
    Main function to orchestrate the creation of the CIK target file.
    """
    print("--- 🚀 LAUNCHING CIK TARGET FILE CREATION SCRIPT ---")

    tickers = get_tickers_from_history()
    if not tickers:
        print("No tickers found. Exiting.")
        return

    all_data = []
    # Use a simple for loop for sequential, controlled execution.
    print(f"\nProcessing {len(tickers)} tickers sequentially...")
    for ticker in tickers:
        try:
            result = fetch_cik_for_ticker(ticker)
            all_data.append(result)
        except Exception as e:
            print(f"  > ❌ Error fetching CIK for {ticker}: {e}")
            all_data.append({'ticker': ticker, 'CIK': 'ERROR'})


    if not all_data:
        print("No CIK data was successfully fetched. Exiting.")
        return

    # --- Save the results to the CSV file ---
    print("\n--- Saving results to CSV ---")
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        return  # Error already printed

    output_dir = os.path.join(base_output_path, "stocks")
    output_path = os.path.join(output_dir, "stocks_filings_targets.csv")

    try:
        # Ensure the directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Create DataFrame and save to CSV, overwriting if it exists
        df = pd.DataFrame(all_data)
        df.to_csv(output_path, index=False)
        print(f"✅ Successfully created and saved target file to: {output_path}")

    except Exception as e:
        print(f"  > ❌ An error occurred while saving the CSV file: {e}")

    print("\n--- ✅ SCRIPT FINISHED ---")


if __name__ == "__main__":
    main()