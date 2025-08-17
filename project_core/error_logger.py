import os
import atexit
from datetime import datetime
import pandas as pd

from . import file_manager

# --- Module-level global list to store errors ---
_ERRORS = []
_IS_REGISTERED = False


def log_error(ticker, fidelity, start_date, end_date, reason, script_name):
    """
    Logs a download failure for later reporting.
    """
    error_details = {
        'log_timestamp_utc': datetime.utcnow().isoformat(),
        'script_name': script_name,
        'ticker': ticker,
        'fidelity': fidelity,
        'failed_chunk_start': start_date,
        'failed_chunk_end': end_date,
        'reason': str(reason)
    }
    _ERRORS.append(error_details)
    print(f"  > 🔴 ERROR LOGGED for {ticker} ({start_date} to {end_date}): {reason}")


def save_errors_to_csv():
    """
    Saves all logged errors to a CSV file. Appends to the file if it exists.
    This function is registered to run automatically on script exit.
    """
    if not _ERRORS:
        print("\n--- No new errors were logged during this session. ---")
        return

    print(f"\n--- Saving {len(_ERRORS)} logged errors to the central error log... ---")

    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("  > 🚨 CRITICAL: Could not find base_output_path in config. Cannot save error log.")
        return

    output_dir = os.path.join(base_output_path, "stocks")
    log_filepath = os.path.join(output_dir, "stocks_trading_history_error_log.csv")

    try:
        new_errors_df = pd.DataFrame(_ERRORS)
        os.makedirs(output_dir, exist_ok=True)

        if os.path.exists(log_filepath):
            new_errors_df.to_csv(log_filepath, mode='a', header=False, index=False)
            print(f"Successfully appended {len(_ERRORS)} errors to: {log_filepath}")
        else:
            new_errors_df.to_csv(log_filepath, index=False)
            print(f"Successfully created new error log at: {log_filepath}")

    except Exception as e:
        print(f"  > 🚨 CRITICAL: Failed to save error log to {log_filepath}: {e}")


def register_error_handler():
    """Registers the save_errors_to_csv function to be called on script exit."""
    global _IS_REGISTERED
    if not _IS_REGISTERED:
        atexit.register(save_errors_to_csv)
        _IS_REGISTERED = True
        print("--- Central error logging handler registered. ---")