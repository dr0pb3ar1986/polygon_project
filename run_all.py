# Import the actual workflow modules from the 'workflows' package
from workflows.stocks import stocks_ticker_list
from workflows.stocks import stocks_ticker_overview
from workflows.stocks import stocks_ticker_relatives
from workflows.stocks import stocks_trading_history
from workflows.stocks import stocks_technicals
from workflows.stocks import stocks_corporate_actions
from workflows.stocks import stocks_fundamentals
from workflows.options import options_ticker_list
from workflows.indices import indices_ticker_list
from workflows.currency import currency_ticker_list
from workflows.options import options_ticker_overview
from workflows.indices import indices_ticker_overview
from workflows.currency import currency_ticker_overview

def main():
    """
    This master script runs all the desired reference data workflows in sequence.
    """
    print("--- 🚀 LAUNCHING MASTER REFERENCE DATA WORKFLOW ---")

    # --- STAGE 1: All Stock workflows (Fast Rate Limit) ---
    print("\n--- Processing All Stock Reference Data ---")
    try:
        stocks_ticker_list.fetch_and_save_all_stock_tickers()
        stocks_ticker_overview.fetch_and_save_all_stock_overviews()
        stocks_ticker_relatives.fetch_and_save_all_stock_relatives()
        print("\n--- ✔️ Stock Reference Data Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the stock workflows: {e} ---")

    # --- STAGE 2: Other Asset Class Ticker Lists (Slow Rate Limit) ---
    print("\n--- Processing Ticker Lists for Other Asset Classes ---")
    try:
        options_ticker_list.fetch_and_save_all_options_tickers()
        indices_ticker_list.fetch_and_save_all_indices_tickers()
        currency_ticker_list.fetch_and_save_all_currency_tickers()
        print("\n--- ✔️ Other Asset Ticker Lists Complete ---")  
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the other ticker list workflows: {e} ---")

    # --- STAGE 3: Other Asset Class Ticker Overviews (Fast Rate Limit) ---
    print("\n--- Processing Ticker Overviews for Other Asset Classes ---")
    try:
        # These workflows depend on the lists from STAGE 2 being successfully generated.
        options_ticker_overview.fetch_and_save_all_options_overviews()
        indices_ticker_overview.fetch_and_save_all_indices_overviews()
        currency_ticker_overview.fetch_and_save_all_currency_overviews()
        print("\n--- ✔️ Other Asset Ticker Overviews Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the other ticker overview workflows: {e} ---")

    # --- STAGE 4: Process Trading History based on Targets ---
    print("\n--- Processing Stock Trading History ---")
    try:
        stocks_trading_history.fetch_and_save_trading_history()
        print("\n--- ✔️ Stock Trading History Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the trading history workflow: {e} ---")

    # --- STAGE 5: Process Technical Indicators based on Targets ---
    print("\n--- Processing Technical Indicators for Stocks ---")
    try:
        stocks_technicals.fetch_and_save_technical_indicators()
        print("\n--- ✔️ Technical Indicators Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the technicals workflow: {e} ---")

    # --- STAGE 6: Process Corporate Actions based on Targets ---
    print("\n--- Processing Corporate Actions for Stocks ---")
    try:
        stocks_corporate_actions.fetch_and_save_corporate_actions()
        print("\n--- ✔️ Corporate Actions Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the corporate actions workflow: {e} ---")

    # --- STAGE 7: Process Fundamentals based on Targets ---
    print("\n--- Processing Fundamentals for Stocks ---")
    try:
        stocks_fundamentals.fetch_and_save_fundamentals()
        print("\n--- ✔️ Fundamentals Complete ---")
    except Exception as e:
        print(f"\n--- ❌ An error occurred during the fundamentals workflow: {e} ---")

    print("\n--- ✅ MASTER WORKFLOW FINISHED ---")


if __name__ == "__main__":
    main()