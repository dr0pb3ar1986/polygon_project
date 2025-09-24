# fetch_filings.py (Place in the root directory)

import pandas as pd
import time
import logging
import sys
import json
import os
import re # <-- Add this import
log = logging.getLogger(__name__)

from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import concurrent.futures
import threading

# Import the updated api_handler
try:
    from project_core import api_handler
    from tqdm import tqdm

    # Import coloredlogs for better logging visuals
    try:
        import coloredlogs

        USE_COLOREDLOGS = True
    except ImportError:
        USE_COLOREDLOGS = False
        print("Note: Install 'coloredlogs' (pip install coloredlogs) for colored logging output.")

except ImportError as e:
    print(f"Error importing dependencies: {e}")
    print(
        "Please ensure project_core is accessible and required libraries (pandas, requests, python-dotenv, python-dateutil, tqdm) are installed.")
    exit()

# --- Configuration ---

INPUT_CSV = r"Y:\Polygon\data_output\stocks\stocks_filings_targets - Copy.csv"
OUTPUT_DIR = r"Y:\Polygon\data_output\stocks\filings"
YEARS_TO_FETCH = 10

# Adjusted Rate Limiting (Aimed at ~8 req/s total)
MAX_WORKERS = 1  # Reduced from 5
WORKER_PACING_SLEEP = 0.2  # Increased from 0.25 (Pacing requests within the worker)
DISCOVERY_PACING_SLEEP = 0.02  # Pacing requests during the sequential discovery phase

# Lock for thread-safe file appending (Crucial for concurrent saving)
write_lock = threading.Lock()

# --- Logging Setup ---
# Configure logging to use colors if available and direct output to STDOUT
if USE_COLOREDLOGS:
    # ✅ Define your custom color scheme here
    custom_level_styles = {
        'debug': {'color': 'magenta'},
        'info': {'color': 'green'}, # Changed from the default black
        'warning': {'color': 'yellow'},
        'error': {'color': 'red'},
        'critical': {'color': 'red', 'bold': True}
    }

    coloredlogs.install(
        level='INFO',
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout,
        level_styles=custom_level_styles # ✅ Pass your custom styles here
    )

else:
    # Fallback to standard logging directed to STDOUT
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# Mappings from SEC API item codes (Key) to standardized schema IDs (Value)
TEN_K_MAPPING = {
    '1': 'Item_1_Business', '1A': 'Item_1A_Risk_Factors', '1B': 'Item_1B_Unresolved_Staff_Comments',
    '2': 'Item_2_Properties', '3': 'Item_3_Legal_Proceedings', '7': 'Item_7_MDandA',
    '7A': 'Item_7A_Market_Risk', '8': 'Item_8_Financial_Statements',
    # Add others as needed based on the documentation provided
}

TEN_Q_MAPPING = {
    # Part 1
    'part1item1': 'P1_Item_1_Financial_Statements', 'part1item2': 'P1_Item_2_MDandA',
    'part1item3': 'P1_Item_3_Market_Risk', 'part1item4': 'P1_Item_4_Controls_and_Procedures',
    # Part 2
    'part2item1': 'P2_Item_1_Legal_Proceedings', 'part2item1a': 'P2_Item_1A_Risk_Factors',
}

# Helper mapping for 8-K items (used for standardization)
EIGHT_K_MAPPING = {
    '1.01': 'Item_1_01_Material_Agreement', '2.01': 'Item_2_01_Acquisition_or_Disposition',
    '2.02': 'Item_2_02_Results_of_Operations', '5.02': 'Item_5_02_Departure_of_Directors_Officers',
    '7.01': 'Item_7_01_Reg_FD_Disclosure', '8.01': 'Item_8_01_Other_Events',
}


# --- Functions ---

def discover_filings(cik_query, cik_meta, ticker):
    """
    Uses the SEC API Query API to find filings. Uses cik_query (unpadded) for the API call
    and cik_meta (padded) for logging.
    """
    logging.info(f"Discovering filings for CIK: {cik_meta} (Ticker: {ticker})")

    # Calculate the date range (Using UTC is generally safer for APIs)
    end_date = datetime.now(timezone.utc)
    start_date = end_date - relativedelta(years=YEARS_TO_FETCH)
    date_range = f"filedAt:[{start_date.strftime('%Y-%m-%d')} TO {end_date.strftime('%Y-%m-%d')}]"

    # Query string uses the cik_query (unpadded)
    query_string = f"cik:{cik_query} AND formType:(\"10-K\" OR \"10-Q\" OR \"8-K\" OR \"10-K/A\" OR \"10-Q/A\" OR \"8-K/A\") AND {date_range}"

    all_filings = []
    start_from = 0
    PAGE_SIZE = 100

    while True:
        payload = {
            "query": {"query_string": {"query": query_string}},
            "from": str(start_from),
            "size": str(PAGE_SIZE),
            "sort": [{"filedAt": {"order": "desc"}}]
        }

        response_data = api_handler.execute_sec_api_query(payload)

        if response_data and 'filings' in response_data:
            filings = response_data['filings']
            all_filings.extend(filings)

            if len(filings) < PAGE_SIZE:
                # Last page reached
                break
            else:
                # Prepare for next page
                start_from += PAGE_SIZE
                time.sleep(0.5)  # Pacing the requests within pagination
        else:
            # This triggers if the API call failed (e.g., after a 429 error handled in api_handler)
            logging.warning(f"Failed to fetch or parse discovery page starting at {start_from} for {ticker}.")
            break

    logging.info(f"Found {len(all_filings)} total filings for {ticker}.")
    return all_filings


def extract_and_process_filing(filing_metadata, cik_meta, ticker):
    """
    Processes a single filing: determines sections, calls Extractor API, and formats the results.
    Uses the cik_meta (padded) for the final metadata.
    """
    form_type = filing_metadata.get('formType', '').upper()
    # Use 'linkToFilingDetails' which points to the actual filing document.
    filing_url = filing_metadata.get('linkToFilingDetails') or filing_metadata.get('linkToHtml')
    accession_no = filing_metadata.get('accessionNo')

    if not filing_url:
        logging.warning(f"Missing primary URL for filing {accession_no}. Skipping.")
        return []

    # Determine which sections to extract based on form type
    sections_to_extract = {}
    if '10-K' in form_type:
        sections_to_extract = TEN_K_MAPPING
    elif '10-Q' in form_type:
        sections_to_extract = TEN_Q_MAPPING
    elif '8-K' in form_type:
        # For 8-K, we only extract items that were actually reported in the filing
        reported_items = filing_metadata.get('items', [])
        for item in reported_items:
            item_clean = item.replace("Item ", "").strip()
            if item_clean in EIGHT_K_MAPPING:
                # The Extractor API expects codes like '1-1' instead of '1.01'
                api_code = item_clean.replace('.', '-')
                sections_to_extract[api_code] = EIGHT_K_MAPPING[item_clean]

    if not sections_to_extract:
        return []

    # Prepare base metadata for JSONL records
    filing_ts_utc = None
    try:
        # Standardize the timestamp to UTC ISO 8601 format
        filed_at = filing_metadata.get('filedAt')
        if filed_at:
            # Handle potential existing timezone info robustly (including 'Z' for UTC)
            dt = datetime.fromisoformat(filed_at.replace('Z', '+00:00'))
            filing_ts_utc = dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, TypeError) as e:
        logging.error(f"Invalid timestamp format for {accession_no}: {filing_metadata.get('filedAt')}. Error: {e}")

    # Extract SIC code (often nested in the entities)
    entities = filing_metadata.get('entities', [])
    sic_code = None
    # Attempt to find the SIC for the entity matching the target CIK
    if isinstance(entities, list):
        for entity in entities:
            # Compare using the padded CIK for accuracy
            try:
                if str(entity.get('cik')).zfill(10) == cik_meta:
                    sic_code = entity.get('sic')
                    break
            except AttributeError:
                continue  # Handle cases where entity might be malformed

    base_record = {
        "accession_number": accession_no,
        "filing_timestamp_utc": filing_ts_utc,
        "period_end_date": filing_metadata.get('periodOfReport'),
        "cik": cik_meta,  # Use the padded CIK
        "ticker": ticker,
        "company_name": filing_metadata.get('companyName'),
        "form_type": form_type,
        "is_amendment": "/A" in form_type,
        "sic_code": sic_code,
        "items_reported": filing_metadata.get('items', []),
        "source_url": filing_url,
        "extraction_method": "sec-api.io_extractor_v1",
        "processing_timestamp_utc": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    # Extract sections
    extracted_records = []
    for api_code, section_id in sections_to_extract.items():
        # Rate limiting safety measure: Pacing the requests within the worker
        time.sleep(WORKER_PACING_SLEEP)

        text_content = api_handler.execute_sec_extractor_request(filing_url, api_code, return_type="text")

        # Basic validation: ensure content exists and meets a minimum length
        if text_content and len(text_content.strip()) > 50:
            record = base_record.copy()
            record['section_id'] = section_id

            # --- UPDATED CLEANING LOGIC ---
            # Normalize whitespace instead of collapsing it entirely

            # 1. Normalize horizontal whitespace: Replace multiple spaces/tabs with a single space
            cleaned_text = re.sub(r'[ \t]+', ' ', text_content.strip())

            # 2. Normalize vertical whitespace: Replace excessive newlines (3 or more)
            #    with a standard paragraph break (2 newlines: \n\n)
            cleaned_text = re.sub(r'(\n\s*){3,}', '\n\n', cleaned_text)

            record['text'] = cleaned_text.strip()
            # --- END OF UPDATE ---

            # The previous flattening logic is now replaced:
            # record['text'] = ' '.join(text_content.strip().split())

            extracted_records.append(record)

    return extracted_records


def save_records(records, ticker):
    """Groups records by year and saves them to the specified JSONL file structure. Thread-safe."""
    if not records:
        return

    # Group by the year of the filing timestamp
    grouped_by_year = {}
    for record in records:
        ts = record.get('filing_timestamp_utc')
        if ts and len(ts) >= 4:
            year = ts[:4]
            if year not in grouped_by_year:
                grouped_by_year[year] = []
            grouped_by_year[year].append(record)
        else:
            logging.warning(f"Skipping record due to missing/invalid timestamp: {record.get('accession_number')}")

    # Ensure the ticker directory exists
    ticker_dir = os.path.join(OUTPUT_DIR, ticker)
    os.makedirs(ticker_dir, exist_ok=True)

    # Save files using a lock for thread safety during concurrent appending
    with write_lock:
        for year, year_records in grouped_by_year.items():
            # Filename format: sec_filings_yyyy_[TICKER].jsonl
            filename = f"sec_filings_{year}_{ticker}.jsonl"
            filepath = os.path.join(ticker_dir, filename)

            try:
                # Append mode ('a') so we don't overwrite data if run multiple times
                with open(filepath, 'a', encoding='utf-8') as f:
                    for record in year_records:
                        f.write(json.dumps(record) + '\n')
            except IOError as e:
                logging.error(f"Error saving file {filepath}: {e}")


def main_workflow():
    """Orchestrates the discovery and extraction process."""
    start_time = time.time()
    logging.info("--- 🚀 Starting SEC Filings Fetch Workflow using sec-api.io ---")

    # 1. Load Targets
    try:
        targets_df = pd.read_csv(INPUT_CSV)

        # New logic: Process CIKs, using existing CIKs if they are valid numbers.
        targets_df['CIK_CLEAN'] = targets_df['CIK'].fillna('NOT_FOUND').astype(str).str.replace(r'\.0$', '', regex=True)
        targets_df['CIK_UNPADDED'] = targets_df['CIK_CLEAN'].apply(
            lambda x: x.lstrip('0') if x.isnumeric() else 'NOT_FOUND')
        targets_df['CIK_PADDED'] = targets_df['CIK_CLEAN'].apply(
            lambda x: x.zfill(10) if x.isnumeric() else 'NOT_FOUND')

        # New column to indicate whether to perform a lookup
        targets_df['NEEDS_LOOKUP'] = ~targets_df['CIK_CLEAN'].apply(str.isnumeric)

        targets = targets_df.to_dict('records')
    except FileNotFoundError:
        logging.error(f"Input CSV not found at {INPUT_CSV}. Exiting.")
        return
    except Exception as e:
        logging.error(f"Error reading input CSV: {e}. Exiting.")
        return

    logging.info(f"Loaded {len(targets)} targets for processing.")

    all_filings_metadata = []

    # 2. Discover Filings (Sequential discovery)
    print("\n--- STAGE 1: Discovering Filings Metadata (10 Years History) ---")
    # Using tqdm for a progress bar during discovery
    for target in tqdm(targets, desc="Discovering Filings"):
        ticker = target['ticker']

        # Determine which CIK to use
        cik_query = target['CIK_UNPADDED']
        cik_meta = target['CIK_PADDED']

        if target['NEEDS_LOOKUP']:
            logging.warning(
                f"Skipping discovery for {ticker} due to invalid CIK: {target['CIK_CLEAN']}. Please correct the input CSV."
            )
            continue

        filings = discover_filings(cik_query, cik_meta, ticker)
        for filing in filings:
            filing['_target_cik_meta'] = cik_meta
            filing['_target_ticker'] = ticker
        all_filings_metadata.extend(filings)

        # Pacing between tickers to prevent 429 errors during discovery
        time.sleep(DISCOVERY_PACING_SLEEP)

    # De-duplicate based on Accession Number AND Ticker.
    unique_filings = {}
    for f in all_filings_metadata:
        key = (f.get('accessionNo'), f.get('_target_ticker'))
        if f.get('accessionNo') and key not in unique_filings:
            unique_filings[key] = f

    logging.info(f"Total unique filings discovered across all targets: {len(unique_filings)}")

    if not unique_filings:
        logging.info("No filings found. Exiting.")
        return

    # 3. Extract Sections (Parallel processing)
    print(f"\n--- STAGE 2: Extracting Sections (Workers: {MAX_WORKERS}) ---")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_filing = {
            executor.submit(extract_and_process_filing, filing, filing['_target_cik_meta'],
                            filing['_target_ticker']): filing
            for filing in unique_filings.values()
        }

        try:
            for future in tqdm(concurrent.futures.as_completed(future_to_filing), total=len(unique_filings),
                               desc="Extracting & Saving"):
                filing = future_to_filing[future]
                try:
                    records = future.result()
                    if records:
                        save_records(records, filing['_target_ticker'])
                except Exception as exc:
                    logging.error(f"Filing {filing.get('accessionNo')} generated an exception during extraction: {exc}")
        except KeyboardInterrupt:
            logging.warning("Workflow interrupted by user. Shutting down workers...")
            executor.shutdown(wait=True, cancel_futures=True)
            logging.info("Shutdown complete.")
            sys.exit(1)

    end_time = time.time()
    logging.info(f"\n--- ✅ Workflow Complete. Total time: {end_time - start_time:.2f} seconds ---")
    logging.info(f"Files saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    # Ensure the API key is available before starting the workflow
    try:
        # This call validates the key in api_handler.py
        api_handler.get_sec_api_key()
        main_workflow()
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
        logging.info("Please ensure SEC_API_KEY is set in your .env file. Exiting workflow.")
    except KeyboardInterrupt:
        logging.warning("Workflow interrupted by user before starting.")
        sys.exit(0)