import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Import the project's own tools
from project_core import file_manager, workflow_helpers, error_logger
from workflows.stocks import stocks_trading_history

# --- USER ACTION REQUIRED ---
# Edit this list to include the specific jobs that failed during the download.
# Use the same 'ticker', 'ticker_fidelity', and 'ticker_duration_months' as
# in your 'stock_ticker_targets.csv' file.
#
# For example:
# FAILED_JOBS = [
#     {
#         'ticker': 'AAPL',
#         'ticker_fidelity': '1 minute',
#         'ticker_duration_months': 3
#     },
#     {
#         'ticker': 'MSFT',
#         'ticker_fidelity': 'tick',
#         'ticker_duration_months': 1
#     },
# ]
#
FAILED_JOBS = [
    # ADD YOUR FAILED JOBS HERE
]
# ----------------------------

def cleanup_and_redownload():
    """
    A one-off script to find and delete potentially corrupt trading history
    files and then trigger a re-download for only those specific jobs.
    """
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING FAILED DOWNLOAD FIXER ---")

    if not FAILED_JOBS:
        print("The 'FAILED_JOBS' list is empty. Please edit the script to add the jobs you want to fix.")
        print("--- ✅ FIXER WORKFLOW FINISHED ---")
        return

    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Error: 'base_output_path' not found in config.ini. Exiting.")
        return

    today = datetime.now()

    # --- STAGE 1: Find and delete all existing files for the failed jobs ---
    print("\n--- STAGE 1: Searching for and deleting old files... ---")
    for job in FAILED_JOBS:
        ticker = job.get('ticker')
        fidelity = str(job.get('ticker_fidelity', ''))
        duration_months = job.get('ticker_duration_months')

        if not all([ticker, fidelity, duration_months]):
            print(f"  > Skipping invalid job due to missing data: {job}")
            continue

        print(f"\n--- Checking files for Ticker: {ticker}, Fidelity: {fidelity} ---")

        try:
            _, timespan = workflow_helpers.parse_historical_fidelity(fidelity)
            if not timespan:
                print(f"  > Could not parse fidelity '{fidelity}'. Skipping deletion check.")
                continue

            overall_start_date = today - relativedelta(months=int(duration_months))
            overall_end_date = today

            # Replicate the monthly chunking logic from the main script to find all possible files
            current_date = overall_start_date
            while current_date <= overall_end_date:
                chunk_start = current_date.replace(day=1)
                chunk_end = chunk_start + relativedelta(months=1) - relativedelta(days=1)
                if chunk_start < overall_start_date: chunk_start = overall_start_date
                if chunk_end > overall_end_date: chunk_end = overall_end_date

                # Replicate the file naming logic to construct the exact path
                fidelity_folder = fidelity.replace(" ", "-")
                output_dir = os.path.join(base_output_path, "stocks", "trading_history", ticker, fidelity_folder)
                filename_base = f"{ticker}_{fidelity_folder}_{chunk_start.strftime('%Y-%m-%d')}_to_{chunk_end.strftime('%Y-%m-%d')}"
                file_extension = ".parquet" if timespan != "day" else ".csv"
                file_to_delete = os.path.join(output_dir, f"{filename_base}{file_extension}")

                if os.path.exists(file_to_delete):
                    print(f"  > Found and DELETING: {os.path.basename(file_to_delete)}")
                    os.remove(file_to_delete)
                else:
                    print(f"  > File not found (OK): {os.path.basename(file_to_delete)}")

                current_date = (current_date.replace(day=1) + relativedelta(months=1))

            # Finally, check for the one-time debug file and delete it if it exists
            fidelity_folder = fidelity.replace(" ", "-")
            output_dir_base = os.path.join(base_output_path, "stocks", "trading_history", ticker, fidelity_folder)
            debug_file_path = os.path.join(output_dir_base, f"{ticker}_{fidelity_folder}_DEBUG.csv")
            if os.path.exists(debug_file_path):
                print(f"  > Found and DELETING debug file: {os.path.basename(debug_file_path)}")
                os.remove(debug_file_path)

        except Exception as e:
            print(f"  > ❌ An error occurred during cleanup for {ticker}: {e}")

    # --- STAGE 2: Re-run the data download for ONLY the failed jobs ---
    print("\n--- STAGE 2: Re-downloading data for specified jobs... ---")

    for job in FAILED_JOBS:
        ticker = job.get('ticker')
        fidelity = str(job.get('ticker_fidelity', ''))
        duration_months = job.get('ticker_duration_months')

        if not all([ticker, fidelity, duration_months]):
            continue # Already warned in stage 1

        try:
            start_date = today - relativedelta(months=int(duration_months))
            # Directly call the original processor function from the workflow
            stocks_trading_history._process_trading_history_job(job, base_output_path, start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))
        except Exception as e:
            print(f"  > ❌ An unexpected error occurred while re-downloading job for {ticker}: {e}")

    print(f"\n--- ✅ FIXER WORKFLOW FINISHED ---")


if __name__ == "__main__":
    cleanup_and_redownload()