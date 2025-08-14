import pandas as pd
import os


def _ensure_directory_exists(filepath):
    """A private helper to check if a directory exists and create it if not."""
    output_dir = os.path.dirname(filepath)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)


def save_to_csv(data, output_path):
    """
    Takes a list of dictionaries, converts it to a pandas DataFrame,
    and saves it as a CSV file.

    :param data: The list of data to save.
    :param output_path: The full path for the output CSV file.
    """
    if not data:
        print("No data provided to save. Skipping file creation.")
        return

    try:
        print(f"Processing {len(data)} records for CSV output...")
        df = pd.DataFrame(data)

        _ensure_directory_exists(output_path)
        df.to_csv(output_path, index=False)
        print(f"Successfully saved data to {output_path}")

    except Exception as e:
        print(f"Error saving data to CSV: {e}")


def save_to_parquet(data, output_path, timestamp_col='t'):
    """
    Takes a list of dictionaries, converts it to a pandas DataFrame with
    proper data types, and saves it as a Parquet file.

    :param data: The list of data to save.
    :param output_path: The full path for the output Parquet file.
    :param timestamp_col: The name of the timestamp column in the raw data (e.g., 't').
    """
    if not data:
        print("No data provided to save. Skipping file creation.")
        return

    try:
        print(f"Processing {len(data)} records for Parquet output...")
        df = pd.DataFrame(data)

        if timestamp_col in df.columns:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='ms', utc=True)
            df.rename(columns={timestamp_col: 'timestamp'}, inplace=True)

        _ensure_directory_exists(output_path)
        df.to_parquet(output_path, engine='pyarrow', compression='snappy')
        print(f"Successfully saved data to {output_path}")

    except Exception as e:
        print(f"Error saving data to Parquet: {e}")


# At the top of data_processor.py, add this import
from project_core import file_manager


# Add this new function to the file
def load_latest_ticker_list(asset_class):
    """
    Finds and loads the latest ticker list CSV for a given asset class.

    :param asset_class: The name of the asset class (e.g., 'stocks', 'options').
    :return: A list of unique tickers, or an empty list if not found.
    """
    print(f"Searching for the latest ticker list for asset class: '{asset_class}'...")

    # Get the base path from our file_manager tool
    base_path = file_manager.get_output_path_from_config()
    if not base_path:
        return []

    folder_name, filename_prefix = file_manager.get_asset_class_paths(asset_class)
    # Construct the search pattern dynamically based on the asset class
    search_pattern = os.path.join(base_path, folder_name, f'{filename_prefix}-*-ticker-list.csv')

    # Use our file_manager tool to find the most recent file
    ticker_file = file_manager.find_latest_file(search_pattern)

    if ticker_file:
        try:
            # Read the CSV and return a list of unique tickers
            tickers_df = pd.read_csv(ticker_file)
            return tickers_df['ticker'].dropna().unique()
        except Exception as e:
            print(f"Error reading ticker file {ticker_file}: {e}")
            return []

    # Return an empty list if no file was found
    return []

# This special block of code is for testing our new functions.


def load_target_tickers(filepath):
    """
    Loads a manually-created CSV of tickers and their data requirements.

    :param filepath: The full path to the target CSV file.
    :return: A list of dictionaries, where each dictionary is a row from the CSV.
    """
    if not os.path.exists(filepath):
        print(f"Error: Target ticker file not found at {filepath}")
        return []
    try:
        df = pd.read_csv(filepath)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error reading target ticker file {filepath}: {e}")
        return []