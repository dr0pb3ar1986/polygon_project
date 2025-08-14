# Import the required currency workflow modules
from workflows.currency import currency_ticker_list
from workflows.currency import currency_ticker_overview


def main():
    """
    This script runs only the currency-related reference data workflows.
    """
    print("--- 🚀 LAUNCHING CURRENCY-ONLY REFERENCE DATA WORKFLOW ---")

    try:
        # Run the workflows in sequence
        currency_ticker_list.fetch_and_save_all_currency_tickers()
        currency_ticker_overview.fetch_and_save_all_currency_overviews()
        print("\n--- ✔️ All Currency Workflows Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the currency workflows: {e} ---")

    print("\n--- ✅ CURRENCY WORKFLOW FINISHED ---")


if __name__ == "__main__":
    main()