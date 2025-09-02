# workflows/stocks/stocks_sec_filings.py

import os
from project_core import api_handler, file_manager, error_logger

def _get_tickers_with_trading_history():
    """Gets a list of tickers from the trading_history directory."""
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        return []

    trading_history_root = os.path.join(base_output_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        print(f"Error: Trading history directory not found at: {trading_history_root}")
        return []

    return [d for d in os.listdir(trading_history_root) if os.path.isdir(os.path.join(trading_history_root, d))]

def fetch_and_save_sec_filings():
    """
    Main workflow to fetch SEC filings for tickers with trading history.
    """
    error_logger.register_error_handler()
    print("--- 🚀 LAUNCHING SEC FILINGS WORKFLOW ---")

    tickers = _get_tickers_with_trading_history()
    if not tickers:
        print("No tickers with trading history found. Exiting SEC filings workflow.")
        return

    print(f"Found {len(tickers)} tickers with trading history. Fetching filings...")

    base_output_path = file_manager.get_output_path_from_config()
    filings_output_dir = os.path.join(base_output_path, "stocks", "filings")

    for ticker in tickers:
        print(f"\n--- Processing Filings for Ticker: {ticker} ---")
        cik = api_handler.get_cik_for_ticker(ticker)
        if not cik:
            print(f"  > Could not find CIK for {ticker}. Skipping.")
            continue

        for filing_type in ['10-K', '10-Q', '8-K']:
            filings = api_handler.get_sec_filings(cik, filing_type)
            if not filings:
                print(f"  > No {filing_type} filings found for {ticker}.")
                continue

            for filing in filings:
                try:
                    # **CORRECTION HERE: Use the direct link to the TXT file**
                    filing_txt_url = filing.get('linkToTxt')
                    if not filing_txt_url:
                        continue

                    # Get the filing content from the direct TXT URL
                    filing_content = api_handler.download_sec_filing(filing_txt_url)

                    if filing_content:
                        # Create the directory structure
                        filing_dir = os.path.join(filings_output_dir, f"{ticker}_{filing_type}")
                        os.makedirs(filing_dir, exist_ok=True)

                        # Save the filing
                        file_name = f"{ticker}_{filing_type}_{filing.get('filedAt').split('T')[0]}.txt"
                        file_path = os.path.join(filing_dir, file_name)

                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(filing_content)
                        print(f"  > Saved: {file_name}")

                except Exception as e:
                    error_logger.log_error(ticker, filing_type, None, None, e, os.path.basename(__file__))
                    print(f"  > ❌ An error occurred while processing a filing for {ticker}: {e}")


    print("\n--- ✅ SEC FILINGS WORKFLOW FINISHED ---")


if __name__ == "__main__":
    fetch_and_save_sec_filings()