import os
import time
import pandas as pd
import concurrent.futures
from project_core import file_manager
from maintenance import discover_filings_to_download, download_discovered_filings

def _run_cleanup_pass():
    """
    Reads the original download list, checks for any files that were missed,
    and re-downloads them at a slower, more conservative pace.
    """
    print("\n--- STAGE 3: Verifying downloads and cleaning up missed files... ---")

    base_output_path = file_manager.get_output_path_from_config()
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")

    if not os.path.exists(download_list_path):
        print("  > Download list not found. Skipping cleanup.")
        return

    try:
        df = pd.read_csv(download_list_path)
        if df.empty:
            print("  > Download list is empty, nothing to clean up.")
            return
        all_jobs = df.to_dict('records')
    except Exception as e:
        print(f"  > Error reading download list for cleanup: {e}")
        return

    missed_files = [job for job in all_jobs if not os.path.exists(job['target_path'])]

    if not missed_files:
        print("  > Verification complete. No missed files found. ---")
        return

    print(f"  > Found {len(missed_files)} files to re-download. Starting cleanup...")

    for job in missed_files:
        download_discovered_filings.download_and_save_filing(job)


def main():
    """
    Orchestrates the entire SEC filings workflow by running the discovery,
    download, and cleanup scripts sequentially.
    """
    print("--- 🚀 LAUNCHING FULL SEC FILINGS WORKFLOW ---")
    start_time = time.time()

    base_output_path = file_manager.get_output_path_from_config()
    download_list_path = os.path.join(base_output_path, "stocks", "stocks_filings_download_list.csv")
    if os.path.exists(download_list_path):
        print("  > Clearing old download list for a fresh run...")
        os.remove(download_list_path)

    print("\n--- STAGE 1: Discovering missing filings... ---")
    try:
        discover_filings_to_download.main()
        print("\n--- ✔️ Discovery Stage Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the discovery stage: {e} ---")
        return

    print("\n--- STAGE 2: Starting high-speed download process... ---")
    try:
        download_discovered_filings.main()
        print("\n--- ✔️ Download Stage Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the download stage: {e} ---")

    _run_cleanup_pass()

    end_time = time.time()
    print(f"\n--- ✅ FULL FILINGS WORKFLOW FINISHED in {end_time - start_time:.2f} seconds ---")


if __name__ == "__main__":
    main()