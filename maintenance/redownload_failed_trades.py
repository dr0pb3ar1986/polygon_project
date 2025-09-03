# maintenance/redownload_failed_trades.py

import os
import pandas as pd
from project_core import api_handler, file_manager, error_logger

def redownload_trade_data(ticker, start_date, end_date):
    """
    Downloads and saves trade data for a specific ticker and date range,
    overwriting any existing file.
    """
    print(f"--- Redownloading trades for {ticker} from {start_date} to {end_date} ---")

    # 1. Get the base output path
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("  > ❌ Could not determine output path. Aborting.")
        return

    # 2. Construct the output path and filename
    output_dir = os.path.join(base_output_path, "stocks", "trading_history", ticker, "tick")
    os.makedirs(output_dir, exist_ok=True)
    file_name = f"{ticker}_tick_{start_date}_to_{end_date}.parquet"
    output_path = os.path.join(output_dir, file_name)

    # 3. Stream the data from the API
    try:
        data_stream = api_handler.stream_trades_data(ticker, start_date, end_date)

        all_trades = []
        total_records = 0
        for page in data_stream:
            all_trades.extend(page)
            total_records += len(page)
            print(f"  > Fetched {len(page)} records... (Total: {total_records})")

        if not all_trades:
            print(f"  > No trade data found for {ticker} in the specified range.")
            return

        # 4. Convert to DataFrame and save as Parquet (overwrites by default)
        # This requires pandas and pyarrow. Ensure you have run: pip install pandas pyarrow
        df = pd.DataFrame(all_trades)
        df.to_parquet(output_path, engine='pyarrow', index=False)

        print(f"  > ✅ Successfully saved {total_records} records to: {output_path}")

    except Exception as e:
        print(f"  > ❌ An error occurred while processing {ticker}: {e}")
        error_logger.log_error(ticker, "trades", start_date, end_date, e, os.path.basename(__file__))

def main():
    """
    Main function to run the redownload process for specific failed files.
    """
    print("--- 🚀 STARTING ONE-OFF REDOWNLOAD SCRIPT ---")
    error_logger.register_error_handler()

    # List of failed downloads to re-process based on logs
    failed_downloads = [
    #    {'ticker': 'GPRE', 'start_date': '2021-11-01', 'end_date': '2021-11-30'},
    #   {'ticker': 'C', 'start_date': '2017-07-01', 'end_date': '2017-07-31'},
    ]

    for download in failed_downloads:
        redownload_trade_data(
            ticker=download['ticker'],
            start_date=download['start_date'],
            end_date=download['end_date']
        )

    print("\n--- ✅ ONE-OFF REDOWNLOAD SCRIPT FINISHED ---")

if __name__ == "__main__":
    main()
