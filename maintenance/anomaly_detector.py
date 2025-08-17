import os
import re
import collections
from datetime import datetime
import pandas as pd

# Import the project's own tools
from project_core import file_manager


def detect_trading_history_anomalies(size_threshold_percent=20.0):
    """
    Scans the trading history directory to find potential data anomalies.
    It flags files that are significantly smaller than their chronological
    neighbors and outputs a CSV report for manual review.
    """
    print("--- 🚀 LAUNCHING TRADING HISTORY ANOMALY DETECTOR ---")

    # --- STAGE 1: Discover All Jobs and Their Files ---
    base_output_path = file_manager.get_output_path_from_config()
    if not base_output_path:
        print("Error: 'base_output_path' not found in config.ini. Exiting.")
        return

    trading_history_root = os.path.join(base_output_path, "stocks", "trading_history")
    if not os.path.isdir(trading_history_root):
        print(f"Error: Trading history directory not found at: {trading_history_root}")
        return

    print(f"Scanning for jobs in: {trading_history_root}")
    # Regex to parse filenames and capture dates for sorting
    file_pattern = re.compile(r"(.+?)_(.+?)_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.(parquet|csv)")
    discovered_jobs = collections.defaultdict(list)

    for ticker_folder in os.listdir(trading_history_root):
        ticker_path = os.path.join(trading_history_root, ticker_folder)
        if os.path.isdir(ticker_path):
            for fidelity_folder in os.listdir(ticker_path):
                fidelity_path = os.path.join(ticker_path, fidelity_folder)
                if os.path.isdir(fidelity_path):
                    for filename in os.listdir(fidelity_path):
                        match = file_pattern.match(filename)
                        if match:
                            start_date = datetime.strptime(match.group(3), '%Y-%m-%d')
                            job_key = (ticker_folder, fidelity_folder.replace("-", " "))
                            discovered_jobs[job_key].append({
                                'path': os.path.join(fidelity_path, filename),
                                'date': start_date
                            })

    if not discovered_jobs:
        print("No jobs found to analyze.")
        return

    print(f"Found {len(discovered_jobs)} jobs. Analyzing file sizes...")
    anomalies = []

    # --- STAGE 2: Analyze File Sizes Within Each Job ---
    for job_key, files in discovered_jobs.items():
        ticker, fidelity = job_key

        # Sort files chronologically
        sorted_files = sorted(files, key=lambda x: x['date'])

        if len(sorted_files) < 2:
            continue  # Need at least two files to compare

        # Get file sizes
        file_sizes = [os.path.getsize(f['path']) for f in sorted_files]

        for i in range(len(sorted_files)):
            current_size = file_sizes[i]
            prev_size = file_sizes[i - 1] if i > 0 else None
            next_size = file_sizes[i + 1] if i < len(sorted_files) - 1 else None

            # Determine the comparison size (the average of its neighbors)
            if prev_size is not None and next_size is not None:
                comparison_size = (prev_size + next_size) / 2
            elif prev_size is not None:
                comparison_size = prev_size
            elif next_size is not None:
                comparison_size = next_size
            else:
                continue

            if comparison_size == 0: continue  # Avoid division by zero

            # Calculate the percentage difference
            percentage_diff = ((comparison_size - current_size) / comparison_size) * 100

            if percentage_diff > size_threshold_percent:
                anomaly = {
                    'ticker': ticker,
                    'fidelity': fidelity,
                    'file_name': os.path.basename(sorted_files[i]['path']),
                    'file_size_kb': round(current_size / 1024, 2),
                    'neighbor_avg_size_kb': round(comparison_size / 1024, 2),
                    'percent_smaller': round(percentage_diff, 2),
                    'reason': f"File is {round(percentage_diff, 2)}% smaller than its neighbors.",
                    'full_path': sorted_files[i]['path']
                }
                anomalies.append(anomaly)
                print(f"  > ❗ ANOMALY FOUND for {ticker}: {os.path.basename(sorted_files[i]['path'])}")

    # --- STAGE 3: Save Report ---
    if not anomalies:
        print("\n--- 🎉 No anomalies found matching the criteria. ---")
        return

    print(f"\n--- STAGE 3: Found {len(anomalies)} potential anomalies. Saving report... ---")

    # Define the output path using your config settings
    output_dir = os.path.join(base_output_path, "stocks")
    os.makedirs(output_dir, exist_ok=True)
    desired_path = os.path.join(output_dir, "stock_ticker_review.csv")

    # Use the file_manager to get a unique path (e.g., ...review(001).csv)
    unique_path = file_manager.get_unique_filepath(desired_path)

    try:
        df = pd.DataFrame(anomalies)
        df.to_csv(unique_path, index=False)
        print(f"Successfully saved anomaly report to: {unique_path}")
    except Exception as e:
        print(f"  > 🚨 Could not save report: {e}")

    print("\n--- ✅ ANOMALY DETECTION FINISHED ---")


if __name__ == "__main__":
    detect_trading_history_anomalies()