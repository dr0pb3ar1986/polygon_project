# Import the required stock workflow modules
from workflows.stocks import stocks_ticker_list
from workflows.stocks import stocks_ticker_overview
from workflows.stocks import stocks_ticker_relatives


def main():
    """
    This script runs only the stock-related reference data workflows.
    """
    print("--- 🚀 LAUNCHING STOCK-ONLY REFERENCE DATA WORKFLOW ---")

    try:
        # Run the workflows in sequence
        stocks_ticker_list.fetch_and_save_all_stock_tickers()
        stocks_ticker_overview.fetch_and_save_all_stock_overviews()
        stocks_ticker_relatives.fetch_and_save_all_stock_relatives()

        print("\n--- ✔️ All Stock Workflows Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the stock workflows: {e} ---")

    print("\n--- ✅ STOCK WORKFLOW FINISHED ---")


if __name__ == "__main__":
    main()