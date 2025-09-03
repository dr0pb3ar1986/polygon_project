# run_filings_workflow.py

from maintenance import discover_filings_to_download
from maintenance import download_discovered_filings
import time
import os
import pandas as pd
import concurrent.futures
from project_core import file_manager


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
        all_jobs = df.to_dict('records')
    except Exception as e:
        print(f"  > Error reading download list for cleanup: {e}")
        return

    # Check which files from the original list are still missing
    missed_files = [job for job in all_jobs if not os.path.exists(job['target_path'])]

    if not missed_files:
        print("  > Verification complete. No missed files found. ---")
        return

    print(f"  > Found {len(missed_files)} files to re-download. Starting cleanup...")

    # Use a slower, more conservative number of workers for the cleanup pass
    max_workers = 20
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # We can reuse the same download function from the main download script
        executor.map(download_discovered_filings.download_and_save_filing, missed_files)

    print("\n--- ✔️ Cleanup Stage Complete ---")


def main():
    """
    Orchestrates the entire SEC filings workflow by running the discovery,
    download, and cleanup scripts sequentially.
    """
    print("--- 🚀 LAUNCHING FULL SEC FILINGS WORKFLOW ---")
    start_time = time.time()

    # --- STAGE 1: Discover what needs to be downloaded ---
    print("\n--- STAGE 1: Discovering missing filings... ---")
    try:
        discover_filings_to_download.main()
        print("\n--- ✔️ Discovery Stage Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the discovery stage: {e} ---")
        return  # Stop the process if discovery fails

    # --- STAGE 2: Download the discovered files ---
    print("\n--- STAGE 2: Starting high-speed download process... ---")
    try:
        download_discovered_filings.main()
        print("\n--- ✔️ Download Stage Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the download stage: {e} ---")

    # --- STAGE 3: Final verification and cleanup ---
    _run_cleanup_pass()

    end_time = time.time()
    print(f"\n--- ✅ FULL FILINGS WORKFLOW FINISHED in {end_time - start_time:.2f} seconds ---")


if __name__ == "__main__":
    main()