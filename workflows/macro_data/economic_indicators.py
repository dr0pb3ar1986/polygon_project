# workflows/macro_data/economic_indicators.py

import os
import pandas as pd
# Import project core tools (assuming polygon_project is set as Sources Root in PyCharm)
from project_core import api_handler, data_processor, file_manager


def fetch_and_save_economic_indicators():
    """
    Main workflow to fetch key macroeconomic indicators and save them as Parquet files.
    """
    print("--- LAUNCHING MACRO ECONOMIC INDICATORS WORKFLOW ---")

    # 1. Determine the output path using the centralized file_manager
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Error: 'base_output_path' not found in config.ini. Exiting.")
        return

    # Define the output directory: [base_path]/macro/indicators/
    output_dir = os.path.join(base_output_path, "macro", "indicators")

    # 2. Define the datasets to fetch
    # Mapping the desired filename (key) to the corresponding API handler function (value)
    datasets = {
        "us_treasury_yields": api_handler.get_treasury_yields,
        "us_inflation_realized": api_handler.get_inflation_realized,
        "us_inflation_expectations": api_handler.get_inflation_expectations,
    }

    # 3. Fetch, process, and save each dataset
    for name, api_func in datasets.items():
        try:
            print(f"\n--- Processing: {name} ---")
            # The API handler functions handle the parameters (limit/sort) internally
            data = api_func()

            if not data:
                print(f" > No data returned for {name}. Skipping save.")
                continue

            # Convert the list of dicts to a DataFrame.
            # The data_processor will handle the standardization.
            df = pd.DataFrame(data)

            # Save the data
            output_path = os.path.join(output_dir, f"{name}.parquet")

            # We call the enhanced save_to_parquet. We must specify timestamp_col='date'.
            data_processor.save_to_parquet(
                df,
                output_path,
                timestamp_col='date'
            )

            # Verification (Note: Column is now 'timestamp' after save_to_parquet finishes)
            # We check the original DataFrame 'df' for the 'date' column for verification before saving.
            if 'date' in df.columns:
                print(f" > Date Range Fetched: {df['date'].min()} to {df['date'].max()}")

        except Exception as e:
            print(f" > An error occurred while processing {name}: {e}")

    print("\n--- MACRO ECONOMIC INDICATORS WORKFLOW FINISHED ---")


# This allows the file to be run directly for testing
if __name__ == "__main__":
    fetch_and_save_economic_indicators()