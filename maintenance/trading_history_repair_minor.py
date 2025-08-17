import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import collections

# Import the project's own tools
from project_core import file_manager, workflow_helpers, error_logger
from workflows.stocks import stocks_trading_history


def repair_minor_trading_history():
    """
    Scans the trading history directory to discover all download jobs. For each
    job, it determines the intended date range and downloads ONLY the specific
    monthly files that are missing, leaving existing files untouched.
    """
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING MINOR TRADING HISTORY REPAIR TOOL ---")

    # --- STAGE 1: Discover All Jobs and Their Files ---
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Error: 'base_output_path' not found in config.ini. Exiting.")
        return

    trading_history_root = os.path.join(base_output_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        print(f"Error: Trading history directory not found at: {trading_history_root}")
        return

    print(f"Scanning for jobs in: {trading_history_root}")
    # Regex to safely parse filenames: Ticker_Fidelity_YYYY-MM-DD_to_YYYY-MM-DD.extension
    file_pattern = re.compile(r"(.+?)_(.+?)_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.(parquet|csv)")
    discovered_jobs = collections.defaultdict(list)

    for ticker_folder in os.listdir(trading_history_root):
        ticker_path = os.path.join(trading_history_root, ticker_folder)
        if os.path.isdir(ticker_path):
            for fidelity_folder in os.listdir(ticker_path):
                fidelity_path = os.path.join(ticker_path, fidelity_folder)
                if os.path.isdir(fidelity_path):
                    for filename in os.listdir(fidelity_path):
                        if file_pattern.match(filename):
                            fidelity_for_api = fidelity_folder.replace("-", " ")
                            job_key = (ticker_folder, fidelity_for_api)
                            discovered_jobs[job_key].append(filename)

    if not discovered_jobs:
        print("No existing trading history jobs found to validate.")
        return

    print(f"Discovered {len(discovered_jobs)} unique jobs to validate.")
    today = datetime.now()
    missing_files_to_download = []

    # --- STAGE 2: Identify Missing Files for Each Job ---
    print("\n--- STAGE 2: Identifying missing monthly files... ---")
    for job_key, filenames in discovered_jobs.items():
        ticker, fidelity = job_key
        print(f"\n--- Validating Job: {ticker} @ {fidelity} ---")

        try:
            # Find the earliest start date from all files for this job
            min_start_date = today
            for filename in filenames:
                match = file_pattern.match(filename)
                start_date_str = match.group(3)
                current_start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                if current_start_date < min_start_date:
                    min_start_date = current_start_date

            print(f"  > Earliest record starts {min_start_date.strftime('%Y-%m-%d')}. Checking for gaps...")

            # Loop through expected months and check if the file exists
            current_date = min_start_date
            while current_date <= today:
                chunk_start = current_date.replace(day=1)
                chunk_end = chunk_start + relativedelta(months=1) - relativedelta(days=1)
                if chunk_start < min_start_date: chunk_start = min_start_date
                if chunk_end > today: chunk_end = today

                _, timespan = workflow_helpers.parse_historical_fidelity(fidelity)
                file_extension = ".parquet" if timespan != "day" else ".csv"
                fidelity_folder = fidelity.replace(" ", "-")

                expected_filename = f"{ticker}_{fidelity_folder}_{chunk_start.strftime('%Y-%m-%d')}_to_{chunk_end.strftime('%Y-%m-%d')}{file_extension}"

                if expected_filename not in filenames:
                    print(f"  > ❌ MISSING: {expected_filename}")
                    # Create a precise download job for this missing chunk
                    download_job = {
                        'job_details': {'ticker': ticker, 'ticker_fidelity': fidelity},
                        'start_date': chunk_start.strftime('%Y-%m-%d'),
                        'end_date': chunk_end.strftime('%Y-%m-%d')
                    }
                    missing_files_to_download.append(download_job)

                current_date = (current_date.replace(day=1) + relativedelta(months=1))

        except Exception as e:
            print(f"  > 🚨 An unexpected error occurred while validating job for {ticker}: {e}")

    # --- STAGE 3: Download Only the Missing Files ---
    if not missing_files_to_download:
        print("\n--- 🎉 All discovered jobs are complete. No repairs needed. ---")
        return

    print(f"\n--- STAGE 3: Found {len(missing_files_to_download)} missing files. Beginning targeted downloads... ---")

    for download in missing_files_to_download:
        job_details = download['job_details']
        start = download['start_date']
        end = download['end_date']

        print(f"\n--- Downloading for {job_details['ticker']} from {start} to {end} ---")
        try:
            stocks_trading_history._process_trading_history_job(job_details, base_output_path, start, end)
        except Exception as e:
            print(f"  > 🚨 An unexpected error occurred while downloading for {job_details['ticker']}: {e}")

    print("\n--- ✅ MINOR REPAIR WORKFLOW FINISHED ---")


if __name__ == "__main__":
    repair_minor_trading_history()