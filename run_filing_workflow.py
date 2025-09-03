from maintenance import discover_filings_to_download
from maintenance import download_discovered_filings
import time

def main():
    """
    This script orchestrates the entire filing download workflow.
    It runs the discovery and download process twice to ensure completeness.
    """
    print("--- 🚀 STARTING FILING DOWNLOAD WORKFLOW ---")

    for i in range(2):
        print(f"\n--- Running Workflow Iteration {i+1} of 2 ---")

        # Step 1: Discover filings to download
        try:
            print("\n--- Running Discovery Script ---")
            discover_filings_to_download.main()
            print("--- ✔️ Discovery Complete ---")
        except Exception as e:
            print(f"--- ❌ An error occurred during discovery: {e} ---")
            continue # Move to next iteration if discovery fails

        # Add a small delay between discovery and download
        time.sleep(2)

        # Step 2: Download the discovered filings
        try:
            print("\n--- Running Download Script ---")
            download_discovered_filings.main()
            print("--- ✔️ Download Complete ---")
        except Exception as e:
            print(f"--- ❌ An error occurred during download: {e} ---")

    print("\n--- ✅ FILING DOWNLOAD WORKFLOW FINISHED ---")

if __name__ == "__main__":
    main()
