# Import the required options workflow modules
from workflows.trading_data.options import options_ticker_list, options_ticker_overview


def main():
    """
    This script runs only the options-related reference data workflows.
    """
    print("--- 🚀 LAUNCHING OPTIONS-ONLY REFERENCE DATA WORKFLOW ---")

    try:
        # Run the workflows in sequence
        options_ticker_list.fetch_and_save_all_options_tickers()
        options_ticker_overview.fetch_and_save_all_options_overviews()
        print("\n--- ✔️ All Options Workflows Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the options workflows: {e} ---")

    print("\n--- ✅ OPTIONS WORKFLOW FINISHED ---")


if __name__ == "__main__":
    main()