import os
import json
import glob


def merge_ticker_filings():
    """
    Merges 10 years of JSONL filing data for multiple tickers into a single
    text file per ticker.
    """
    # --- IMPORTANT: Update these paths to match your folders ---
    source_directory = r'Y:\Polygon\data_output\stocks\filings'
    destination_directory = r'C:\Users\andre\OneDrive\trading'

    # --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

    if not os.path.exists(source_directory):
        print(f"Error: Source directory not found at '{source_directory}'")
        return

    # Create the destination folder if it doesn't exist
    os.makedirs(destination_directory, exist_ok=True)
    print(f"Output will be saved to: {destination_directory}")

    # Get a list of all ticker subdirectories
    ticker_folders = [f.path for f in os.scandir(source_directory) if f.is_dir()]

    if not ticker_folders:
        print(f"No ticker subfolders found in '{source_directory}'. Please check the path.")
        return

    print(f"Found {len(ticker_folders)} tickers to process...")

    for ticker_folder in ticker_folders:
        ticker = os.path.basename(ticker_folder)
        output_filename = os.path.join(destination_directory, f"{ticker}_filings.txt")

        # Find all JSONL files for the current ticker
        jsonl_files = glob.glob(os.path.join(ticker_folder, '*.jsonl'))

        if not jsonl_files:
            print(f"  - No .jsonl files found for {ticker}. Skipping.")
            continue

        print(f"  - Processing {ticker}... Found {len(jsonl_files)} files.")

        # Open the destination file and write all content into it
        with open(output_filename, 'w', encoding='utf-8') as outfile:
            # Sort files to process them in a consistent order (e.g., by year)
            jsonl_files.sort()

            for filename in jsonl_files:
                try:
                    with open(filename, 'r', encoding='utf-8') as infile:
                        for line in infile:
                            try:
                                # Load the JSON object from the line
                                data = json.loads(line)

                                # Extract relevant info to add as a header
                                form_type = data.get('form_type', 'N/A')
                                period_end = data.get('period_end_date', 'N/A')
                                section = data.get('section_id', 'N/A')
                                text = data.get('text', '')

                                # Write a separator and the text content to the output file
                                outfile.write(
                                    f"\n\n--- FILING: {ticker} | FORM: {form_type} | PERIOD: {period_end} | SECTION: {section} ---\n\n")
                                outfile.write(text)

                            except json.JSONDecodeError:
                                # This line isn't valid JSON, skip it
                                continue
                except Exception as e:
                    print(f"    - Could not process file {filename}: {e}")

    print("\nProcessing complete!")


# Run the function
merge_ticker_filings()