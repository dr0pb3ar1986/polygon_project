# Import the required indices workflow modules
from workflows.indices import indices_ticker_list
from workflows.indices import indices_ticker_overview


def main():
    """
    This script runs only the indices-related reference data workflows.
    """
    print("--- 🚀 LAUNCHING INDICES-ONLY REFERENCE DATA WORKFLOW ---")

    try:
        # Run the workflows in sequence
        ticker_list.fetch_and_save_all_indices_tickers()
        ticker_overview.fetch_and_save_all_indices_overviews()
        print("\n--- ✔️ All Indices Workflows Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the indices workflows: {e} ---")

    print("\n--- ✅ INDICES WORKFLOW FINISHED ---")


if __name__ == "__main__":
    main()