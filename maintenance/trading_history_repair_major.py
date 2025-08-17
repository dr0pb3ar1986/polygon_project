import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import collections

# Import the project's own tools
from project_core import file_manager, workflow_helpers, error_logger
from workflows.stocks import stocks_trading_history


def scan_and_repair_trading_history():
    """
    Scans the existing trading_history directory to discover all download jobs
    (ticker/fidelity combinations). For each job, it determines the intended
    date range based on the earliest file found and checks for any missing

    monthly files up to the current date. If a job is found to be incomplete,
    all its existing files are deleted and the entire job is re-downloaded.
    """
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING DIRECTORY-BASED HISTORY SCANNER AND REPAIR TOOL ---")

    # --- STAGE 1: Discover All Jobs by Scanning the Directory ---
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Error: 'base_output_path' not found in config.ini. Exiting.")
        return

    trading_history_root = os.path.join(base_output_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        print(f"Error: Trading history directory not found at: {trading_history_root}")
        print("--- ✅ SCAN FINISHED ---")
        return

    print(f"Scanning for jobs in: {trading_history_root}")
    discovered_jobs = collections.defaultdict(list)
    # Regex to safely parse filenames: Ticker_Fidelity_YYYY-MM-DD_to_YYYY-MM-DD.extension
    file_pattern = re.compile(r"(.+?)_(.+?)_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.(parquet|csv)")

    for ticker_folder in os.listdir(trading_history_root):
        ticker_path = os.path.join(trading_history_root, ticker_folder)
        if not os.path.isdir(ticker_path):
            continue
        for fidelity_folder in os.listdir(ticker_path):
            fidelity_path = os.path.join(ticker_path, fidelity_folder)
            if not os.path.isdir(fidelity_path):
                continue
            for filename in os.listdir(fidelity_path):
                match = file_pattern.match(filename)
                if match:
                    # The fidelity on disk uses hyphens, convert back to spaces for the API
                    fidelity_for_api = fidelity_folder.replace("-", " ")
                    job_key = (ticker_folder, fidelity_for_api)
                    # Store the full path for potential deletion later
                    discovered_jobs[job_key].append(os.path.join(fidelity_path, filename))

    if not discovered_jobs:
        print("No existing trading history jobs found to validate.")
        print("--- ✅ SCAN FINISHED ---")
        return

    print(f"Discovered {len(discovered_jobs)} unique jobs to validate.")
    today = datetime.now()
    incomplete_jobs_to_fix = []

    # --- STAGE 2: Validate Each Discovered Job ---
    print("\n--- STAGE 2: Validating completeness of each job... ---")
    for job_key, file_paths in discovered_jobs.items():
        ticker, fidelity = job_key
        is_job_complete = True
        print(f"\n--- Validating Job: {ticker} @ {fidelity} ---")

        try:
            # Find the earliest start date from all files for this job
            min_start_date = today
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                match = file_pattern.match(filename)
                start_date_str = match.group(3)
                current_start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                if current_start_date < min_start_date:
                    min_start_date = current_start_date

            print(
                f"  > Earliest record found starts on: {min_start_date.strftime('%Y-%m-%d')}. Validating all months from then until today...")

            # Check for all expected monthly files from the earliest date to today
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
                expected_filepath = os.path.join(trading_history_root, ticker, fidelity_folder, expected_filename)

                if not os.path.exists(expected_filepath):
                    print(f"  > ❌ MISSING FILE: {expected_filename}")
                    is_job_complete = False

                current_date = (current_date.replace(day=1) + relativedelta(months=1))

            if not is_job_complete:
                # Calculate the total duration needed for the re-download
                r = relativedelta(today, min_start_date)
                duration_months = r.years * 12 + r.months + 1  # Add 1 to be inclusive

                job_details = {
                    'ticker': ticker,
                    'ticker_fidelity': fidelity,
                    'ticker_duration_months': duration_months,
                    'files_to_delete': file_paths  # Keep track of all files to delete
                }
                incomplete_jobs_to_fix.append(job_details)
                print(
                    f"  > ❗ Job for {ticker} is incomplete. Flagging for a full re-download of {duration_months} months.")
            else:
                print(f"  > ✅ Job for {ticker} is complete.")

        except Exception as e:
            print(f"  > 🚨 An unexpected error occurred while validating job for {ticker}: {e}")

    # --- STAGE 3: Cleanup and Re-download Incomplete Jobs ---
    if not incomplete_jobs_to_fix:
        print("\n--- 🎉 All discovered jobs are validated and complete. No action needed. ---")
        print("--- ✅ SCAN FINISHED ---")
        return

    print(f"\n--- STAGE 3: Found {len(incomplete_jobs_to_fix)} incomplete jobs. Cleaning up and re-downloading... ---")

    for job in incomplete_jobs_to_fix:
        print(f"\n--- Processing Fix for: {job['ticker']} @ {job['ticker_fidelity']} ---")

        # STAGE 3a: Cleanup all discovered files for this job
        print(f"  > Deleting {len(job['files_to_delete'])} existing files for this job to ensure a clean start...")
        for file_path in job['files_to_delete']:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"  > Could not delete file {os.path.basename(file_path)}: {e}")

        # Also delete the debug file, just in case
        fidelity_folder = job['ticker_fidelity'].replace(" ", "-")
        debug_file_path = os.path.join(trading_history_root, job['ticker'], fidelity_folder,
                                       f"{job['ticker']}_{fidelity_folder}_DEBUG.csv")
        if os.path.exists(debug_file_path):
            try:
                os.remove(debug_file_path)
                print("  > Deleted associated debug file.")
            except Exception as e:
                print(f"  > Could not delete debug file: {e}")

        # STAGE 3b: Re-download the data for the full, corrected duration
        try:
            start_date = today - relativedelta(months=int(job['ticker_duration_months']))
            stocks_trading_history._process_trading_history_job(job, base_output_path, start_date.strftime('%Y-%m-%d'),
                                                                today.strftime('%Y-%m-%d'))
        except Exception as e:
            print(f"  > 🚨 An unexpected error occurred while re-downloading job for {job['ticker']}: {e}")

    print("\n--- ✅ SCAN AND REPAIR WORKFLOW FINISHED ---")


if __name__ == "__main__":
    scan_and_repair_trading_history()