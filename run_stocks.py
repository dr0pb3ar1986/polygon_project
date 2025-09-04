# Import all required stock workflow modules
from workflows.trading_data.stocks import stocks_ticker_overview, stocks_corporate_actions, stocks_ticker_relatives, \
    stocks_fundamentals
from workflows.trading_data.stocks import stocks_technicals, stocks_trading_history, stocks_ticker_list


def main():
    """
    This script runs all stock-related reference and historical data workflows.
    """
    print("--- 🚀 LAUNCHING ALL-STOCKS DATA WORKFLOW ---")

    try:
        # --- Stage 1: Reference Data ---
        print("\n--- Processing Stock Reference Data ---")
        stocks_ticker_list.fetch_and_save_all_stock_tickers()
        stocks_ticker_overview.fetch_and_save_all_stock_overviews()
        stocks_ticker_relatives.fetch_and_save_all_stock_relatives()
        print("\n--- ✔️ Stock Reference Data Complete ---")

        # --- Stage 2: Target-Based Historical Data ---
        print("\n--- Processing Stock Trading History ---")
        stocks_trading_history.fetch_and_save_trading_history()
        print("\n--- ✔️ Stock Trading History Complete ---")

        print("\n--- Processing Technical Indicators for Stocks ---")
        stocks_technicals.fetch_and_save_technical_indicators()
        print("\n--- ✔️ Technical Indicators Complete ---")

        print("\n--- Processing Corporate Actions for Stocks ---")
        stocks_corporate_actions.fetch_and_save_corporate_actions()
        print("\n--- ✔️ Corporate Actions Complete ---")

        print("\n--- Processing Fundamentals for Stocks ---")
        stocks_fundamentals.fetch_and_save_fundamentals()
        print("\n--- ✔️ Fundamentals Complete ---")

    except Exception as e:
        print(f"\n--- ❌ A top-level error occurred during the stock workflows: {e} ---")

    print("\n--- ✅ ALL-STOCKS WORKFLOW FINISHED ---")


if __name__ == "__main__":
    main()

    from workflows.trading_data.stocks import superceeded_stocks_sec_filings


    def main():
        """
        This script runs all stock-related reference and historical data workflows.
        """
        print("--- 🚀 LAUNCHING ALL-STOCKS DATA WORKFLOW ---")

        try:
            # ... (keep all your existing workflow calls) ...

            print("\n--- Processing SEC Filings for Stocks ---")
            stocks_sec_filings.fetch_and_save_sec_filings()
            print("\n--- ✔️ SEC Filings Complete ---")


        except Exception as e:
            print(f"\n--- ❌ A top-level error occurred during the stock workflows: {e} ---")

        print("\n--- ✅ ALL-STOCKS WORKFLOW FINISHED ---")


    if __name__ == "__main__":
        main()