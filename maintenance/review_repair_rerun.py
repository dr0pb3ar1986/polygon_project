import os
import re
from datetime import datetime
import pandas as pd

# Import the project's own tools
from project_core import file_manager, workflow_helpers, error_logger
from workflows.stocks import stocks_trading_history


def review_and_repair_anomalies():
    """
    Finds the latest anomaly review CSV, reads the list of files to fix
    (i.e., those not marked as 'ignore'), deletes them, and re-downloads
    the specific monthly chunk for each.
    """
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING ANOMALY REVIEW AND REPAIR TOOL ---")

    # --- STAGE 1: Find and Load the Latest Review File ---
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Error: 'base_output_path' not found in config.ini. Exiting.")
        return

    # Use the file_manager to find the latest version of the review file
    search_pattern = os.path.join(base_output_path, "stocks", "stock_ticker_review*.csv")
    latest_review_file = file_manager.find_latest_file(search_pattern)

    if not latest_review_file:
        print("Error: No 'stock_ticker_review.csv' file found. Run anomaly_detector.py first.")
        return

    print(f"Found latest review file: {os.path.basename(latest_review_file)}")

    try:
        df = pd.read_csv(latest_review_file)
        # Filter out rows where 'ignore' column has any text
        df_to_fix = df[df['ignore'].isnull() | (df['ignore'] == '')].copy()
    except Exception as e:
        print(f"Error reading or processing the review file: {e}")
        return

    if df_to_fix.empty:
        print("No anomalies marked for fixing in the review file.")
        print("--- ✅ REPAIR WORKFLOW FINISHED ---")
        return

    print(f"Found {len(df_to_fix)} anomalies to repair.")

    # --- STAGE 2: Delete and Re-download Flagged Files ---
    print("\n--- STAGE 2: Processing files marked for repair... ---")
    file_pattern = re.compile(r".*_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.(parquet|csv)")

    for index, row in df_to_fix.iterrows():
        file_path = row['full_path']
        ticker = row['ticker']
        fidelity = row['fidelity']
        filename = os.path.basename(file_path)

        print(f"\n--- Repairing: {filename} ---")

        # STAGE 2a: Delete the anomalous file
        if os.path.exists(file_path):
            try:
                print(f"  > Deleting file: {filename}")
                os.remove(file_path)
            except Exception as e:
                print(f"  > 🚨 Could not delete file: {e}. Skipping re-download.")
                continue
        else:
            print("  > File already deleted or moved. Skipping deletion.")

        # STAGE 2b: Re-download the specific chunk
        try:
            match = file_pattern.match(filename)
            if not match:
                print(f"  > 🚨 Could not parse date range from filename: {filename}. Cannot re-download.")
                continue

            start_date = match.group(1)
            end_date = match.group(2)

            job_details = {'ticker': ticker, 'ticker_fidelity': fidelity}

            print(f"  > Re-downloading data for {ticker} from {start_date} to {end_date}...")
            stocks_trading_history._process_trading_history_job(job_details, base_output_path, start_date, end_date)

        except Exception as e:
            print(f"  > 🚨 An unexpected error occurred while re-downloading for {ticker}: {e}")

    print("\n--- ✅ REPAIR WORKFLOW FINISHED ---")


if __name__ == "__main__":
    review_and_repair_anomalies()