# project_core/api_handler.py

import os
import requests
from datetime import datetime
from dotenv import load_dotenv
import re
from bs4 import BeautifulSoup
import logging  # <-- Added Import
import time  # <-- Added Import

# --- Global Configuration and Session for efficiency ---
SESSION = requests.Session()
BASE_URL = "https://api.polygon.io"
REQUEST_TIMEOUT = (5, 30)

# SEC API Specifics
SEC_API_BASE_URL = "https://api.sec-api.io"
SEC_EXTRACTOR_ENDPOINT = f"{SEC_API_BASE_URL}/extractor"

# --- Module-level cache for the API key to avoid re-reading the file ---
_API_KEY = None
_SEC_API_KEY = None


def get_api_key():
    """
    Loads the API key from the .env file, caching it for subsequent calls.
    """
    global _API_KEY
    if _API_KEY is None:
        load_dotenv()
        _API_KEY = os.getenv("POLYGON_API_KEY")
        if not _API_KEY:
            # Standardized to use logging
            logging.warning("Error: 'POLYGON_API_KEY' not found. Make sure it is set correctly in your .env file.")
            return None
    return _API_KEY


def get_sec_api_key():
    """
    Loads the SEC API key from the .env file, caching it for subsequent calls.
    """
    global _SEC_API_KEY
    if _SEC_API_KEY is None:
        load_dotenv()
        _SEC_API_KEY = os.getenv("SEC_API_KEY")
        if not _SEC_API_KEY:
            logging.error("SEC_API_KEY not found. Make sure it is set correctly in your .env file.")
            # Raise an exception as the script cannot proceed
            raise ValueError("SEC_API_KEY is required but not found.")
    return _SEC_API_KEY


def _execute_session_get(url, params=None, headers=None):
    """
    A private helper to execute a GET request and handle common exceptions.
    """
    try:
        response = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT, headers=headers)
        response.raise_for_status()
        # Check if response is JSON before trying to decode
        if 'application/json' in response.headers.get('Content-Type', ''):
            return response.json()
        return response.text  # Return as text for non-JSON responses
    except requests.exceptions.Timeout:
        # Standardized to use logging
        logging.warning(f"  > Request timed out for URL: {url}")
        return None
    except requests.exceptions.RequestException as e:
        # Standardized to use logging
        logging.error(f"  > Error during API request for {url}: {e}")
        return None


# Note: The following Polygon functions still use 'print' for progress updates (e.g., "Fetching page X...").
# This is kept as-is because 'print' is often used for user-facing progress in existing scripts,
# while 'logging' (configured in the main workflow script) handles system events/errors.

def _fetch_paginated_data_by_next_url(endpoint_path, params, results_extractor):
    """
    A generic helper for endpoints that use 'next_url' for pagination.
    """
    api_key = get_api_key()
    if not api_key:
        return []

    all_results = []
    request_params = params.copy()
    request_params['apiKey'] = api_key
    current_url = BASE_URL + endpoint_path
    page_count = 1

    while current_url:
        print(f"Fetching page {page_count}...")
        data = _execute_session_get(current_url, params=request_params if page_count == 1 else None)
        if data is None or not isinstance(data, dict):
            break

        results_on_page = results_extractor(data)
        all_results.extend(results_on_page)
        print(f"  > Found {len(results_on_page)} results on this page. Total results: {len(all_results)}")

        next_url = data.get('next_url')
        if next_url:
            current_url = f"{next_url}&apiKey={api_key}"
            page_count += 1
        else:
            current_url = None
    return all_results


def _stream_paginated_data_by_next_url(endpoint_path, params, results_extractor):
    """
    A generator-based helper for endpoints that use 'next_url' for pagination.
    This yields pages of results to keep memory usage low.
    """
    api_key = get_api_key()
    if not api_key:
        return

    request_params = params.copy()
    request_params['apiKey'] = api_key
    current_url = BASE_URL + endpoint_path
    page_count = 1

    while current_url:
        print(f"Streaming page {page_count}...")
        data = _execute_session_get(current_url, params=request_params if page_count == 1 else None)
        if data is None or not isinstance(data, dict):
            break

        results_on_page = results_extractor(data)
        if results_on_page:
            print(f"  > Yielding {len(results_on_page)} results from this page.")
            yield results_on_page

        next_url = data.get('next_url')
        if next_url:
            current_url = f"{next_url}&apiKey={api_key}"
            page_count += 1
        else:
            current_url = None


def get_paginated_data(endpoint_path, params):
    """
    Fetches data from a paginated Polygon.io endpoint.
    """
    return _fetch_paginated_data_by_next_url(
        endpoint_path,
        params,
        lambda data: data.get('results', [])
    )


def _make_api_request(endpoint_path, params=None, log_message="", success_key='results', default_return_value=None):
    """
    A centralized helper to make a single API request.
    """
    api_key = get_api_key()
    if not api_key:
        return default_return_value

    request_params = params.copy() if params else {}
    request_params['apiKey'] = api_key
    full_url = BASE_URL + endpoint_path

    if log_message:
        print(log_message)

    data = _execute_session_get(full_url, params=request_params)

    if data is not None and isinstance(data, dict):
        return data.get(success_key, default_return_value)
    else:
        return default_return_value


def get_ticker_details(ticker):
    return _make_api_request(
        endpoint_path=f"/v3/reference/tickers/{ticker}",
        log_message=f"  > Fetching details for {ticker}...",
        default_return_value=None
    )


def get_related_tickers(ticker):
    return _make_api_request(
        endpoint_path=f"/v1/related-companies/{ticker}",
        log_message=f"  > Fetching relatives for {ticker}...",
        default_return_value=[]
    )


def get_option_contract_details(option_ticker):
    return _make_api_request(
        endpoint_path=f"/v3/reference/options/contracts/{option_ticker}",
        log_message=f"  > Fetching details for {option_ticker}...",
        default_return_value=None
    )


def get_aggregate_data(ticker, multiplier, timespan, from_date, to_date, params=None):
    api_key = get_api_key()
    if not api_key:
        return []

    all_results = []
    page_count = 1
    limit = 50000

    try:
        from_timestamp = int(datetime.strptime(from_date, '%Y-%m-%d').timestamp() * 1000)
        to_timestamp = int(datetime.strptime(to_date, '%Y-%m-%d').timestamp() * 1000)
    except ValueError:
        # Standardized to use logging
        logging.error(f"  > Invalid date format for {ticker}. Expected YYYY-MM-DD.")
        return []

    current_from = from_timestamp
    print(f"--- Fetching Aggregates for {ticker} from {from_date} to {to_date} ---")

    while current_from <= to_timestamp:
        endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{current_from}/{to_timestamp}"
        request_params = params.copy() if params else {}
        request_params['adjusted'] = 'true'
        request_params['sort'] = 'asc'
        request_params['limit'] = limit
        request_params['apiKey'] = api_key
        full_url = BASE_URL + endpoint
        print(f"Fetching page {page_count}...")
        data = _execute_session_get(full_url, params=request_params)

        if data is None or not isinstance(data, dict):
            break
        results_on_page = data.get('results', [])
        if not results_on_page:
            break

        all_results.extend(results_on_page)
        print(f"  > Found {len(results_on_page)} results on this page. Total results: {len(all_results)}")

        if len(results_on_page) < limit:
            break

        # Robustness check for timestamp key 't'
        if results_on_page[-1] and 't' in results_on_page[-1]:
            last_timestamp = results_on_page[-1]['t']
            current_from = last_timestamp + 1
        else:
            logging.warning(f"Timestamp key 't' missing in the last result for {ticker}. Stopping pagination.")
            break

        page_count += 1
    return all_results


def stream_aggregate_data(ticker, multiplier, timespan, from_date, to_date, params=None):
    """
    Streams aggregate data page by page to conserve memory.
    """
    api_key = get_api_key()
    if not api_key:
        return

    try:
        from_timestamp = int(datetime.strptime(from_date, '%Y-%m-%d').timestamp() * 1000)
        to_timestamp = int(datetime.strptime(to_date, '%Y-%m-%d').timestamp() * 1000)
    except ValueError:
        # Standardized to use logging
        logging.error(f"  > Invalid date format for {ticker}. Expected YYYY-MM-DD.")
        return

    current_from = from_timestamp
    limit = 50000
    print(f"--- Streaming Aggregates for {ticker} from {from_date} to {to_date} ---")

    while current_from <= to_timestamp:
        endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{current_from}/{to_timestamp}"
        request_params = params.copy() if params else {}
        request_params.update({'adjusted': 'true', 'sort': 'asc', 'limit': limit, 'apiKey': api_key})
        full_url = BASE_URL + endpoint

        data = _execute_session_get(full_url, params=request_params)
        if data is None or not isinstance(data, dict):
            break

        results_on_page = data.get('results', [])
        if not results_on_page:
            break

        yield results_on_page

        # Robustness check for timestamp key 't'
        if results_on_page[-1] and 't' in results_on_page[-1]:
            last_timestamp = results_on_page[-1]['t']
            current_from = last_timestamp + 1
        else:
            logging.warning(f"Timestamp key 't' missing in the last result for {ticker}. Stopping pagination.")
            break


def get_trades_data(ticker, from_date, to_date, params=None):
    endpoint = f"/v3/trades/{ticker}"
    request_params = params.copy() if params else {}
    request_params['timestamp.gte'] = f"{from_date}T00:00:00Z"
    request_params['timestamp.lte'] = f"{to_date}T23:59:59Z"
    request_params['limit'] = 50000
    print(f"--- Fetching Trades for {ticker} from {from_date} to {to_date} ---")
    return get_paginated_data(endpoint, request_params)


def stream_trades_data(ticker, from_date, to_date, params=None):
    """Streams trade data page by page to conserve memory."""
    endpoint = f"/v3/trades/{ticker}"
    request_params = params.copy() if params else {}
    request_params['timestamp.gte'] = f"{from_date}T00:00:00Z"
    request_params['timestamp.lte'] = f"{to_date}T23:59:59Z"
    request_params['limit'] = 50000
    print(f"--- Streaming Trades for {ticker} from {from_date} to {to_date} ---")
    yield from _stream_paginated_data_by_next_url(endpoint, request_params, lambda data: data.get('results', []))


def get_technical_indicator_data(indicator_name, ticker, timespan, from_date, to_date, params=None):
    endpoint_path = f"/v1/indicators/{indicator_name}/{ticker}"
    request_params = params.copy() if params else {}
    request_params['timespan'] = timespan
    request_params['timestamp.gte'] = from_date
    request_params['timestamp.lte'] = to_date
    request_params['limit'] = 5000
    request_params['sort'] = 'asc'
    print(f"--- Fetching {indicator_name.upper()} for {ticker} from {from_date} to {to_date} ---")
    return _fetch_paginated_data_by_next_url(
        endpoint_path,
        request_params,
        lambda data: data.get('results', {}).get('values', [])
    )


# ... (Keep all other specific Polygon functions: get_sma_data, get_ema_data, etc.) ...
def get_sma_data(ticker, timespan, from_date, to_date, window=50):
    return get_technical_indicator_data('sma', ticker, timespan, from_date, to_date, params={'window': window})


def get_ema_data(ticker, timespan, from_date, to_date, window=50):
    return get_technical_indicator_data('ema', ticker, timespan, from_date, to_date, params={'window': window})


def get_macd_data(ticker, timespan, from_date, to_date, short_window=12, long_window=26, signal_window=9):
    params = {'short_window': short_window, 'long_window': long_window, 'signal_window': signal_window}
    return get_technical_indicator_data('macd', ticker, timespan, from_date, to_date, params)


def get_rsi_data(ticker, timespan, from_date, to_date, window=14):
    return get_technical_indicator_data('rsi', ticker, timespan, from_date, to_date, params={'window': window})


def get_ipos_data(ticker, from_date, to_date):
    params = {'ticker': ticker, 'date.gte': from_date, 'date.lte': to_date, 'limit': 1000}
    return get_paginated_data('/v3/reference/ipo', params)


def get_splits_data(ticker, from_date, to_date):
    params = {'ticker': ticker, 'execution_date.gte': from_date, 'execution_date.lte': to_date, 'limit': 1000}
    return get_paginated_data('/v3/reference/splits', params)


def get_dividends_data(ticker, from_date, to_date):
    params = {'ticker': ticker, 'ex_dividend_date.gte': from_date, 'ex_dividend_date.lte': to_date, 'limit': 1000}
    return get_paginated_data('/v3/reference/dividends', params)


def get_ticker_events_data(ticker, from_date, to_date):
    params = {'ticker': ticker, 'date.gte': from_date, 'date.lte': to_date, 'limit': 1000}
    return get_paginated_data('/v3/reference/ticker-events', params)


def get_financials_data(ticker, from_date, to_date):
    params = {'ticker': ticker, 'filing_date.gte': from_date, 'filing_date.lte': to_date, 'limit': 100}
    return get_paginated_data('/vX/reference/financials', params)


def get_short_interest_data(ticker, from_date, to_date):
    params = {'report_date.gte': from_date, 'report_date.lte': to_date, 'limit': 1000}
    return get_paginated_data(f'/v3/short-interest/{ticker}', params)


def get_short_volume_data(ticker, from_date, to_date):
    params = {'date.gte': from_date, 'date.lte': to_date, 'limit': 1000}
    return get_paginated_data(f'/v3/short-volume/{ticker}', params)


# --- SEC API Functions ---

def get_cik_for_ticker(ticker):
    """
    Converts a ticker to a CIK using the sec-api.io mapping API with a fallback
    to the public SEC EDGAR API.
    """
    api_key = get_sec_api_key()
    if not api_key:
        return None

    # --- Primary Lookup: sec-api.io ---
    try:
        url = f"https://api.sec-api.io/mapping/ticker/{ticker}?token={api_key}"
        data = _execute_session_get(url)
        if data and isinstance(data, list) and len(data) > 0:
            cik = data[0].get('cik')
            if cik:
                return str(cik)
    except Exception as e:
        logging.warning(f"  > Primary CIK lookup for {ticker} failed with error: {e}. Trying fallback.")

    # --- Fallback Lookup: Public SEC EDGAR API ---
    logging.info(f"  > Attempting fallback CIK lookup for {ticker} using SEC.gov API...")
    try:
        # The SEC has a public API that maps tickers to CIKs.
        # We search for the ticker in the master list.
        url = f"https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers={'User-Agent': 'YourCompanyName your_email@example.com'})
        response.raise_for_status()
        ticker_list = response.json()

        for entry in ticker_list:
            if entry.get('ticker') == ticker.upper():
                cik = str(entry.get('cik_str')).zfill(10)
                logging.info(f"  > Fallback successful: Found CIK {cik} for {ticker}.")
                return cik

    except requests.exceptions.RequestException as e:
        logging.error(f"  > Failed to access public SEC API for CIK lookup: {e}")

    logging.warning(f"  > CIK not found for ticker: {ticker} after both attempts.")
    return None


def execute_sec_api_query(payload):
    """
    Executes a POST request to the SEC API Query endpoint (used for discovery).
    """
    api_key = get_sec_api_key()
    # The Query API uses POST to the base URL with the token in the query string.
    url = f"{SEC_API_BASE_URL}?token={api_key}"

    try:
        # Must use SESSION.post for POST requests
        response = SESSION.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error during SEC API Query request: {e.response.status_code} - {e}")
        if e.response.status_code == 429:
            logging.warning("Rate limit hit (429) during Query API. Sleeping for 30 seconds.")
            time.sleep(30)
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during SEC API Query request: {e}")
        return None


def execute_sec_extractor_request(filing_url, item_code, return_type="text"):
    """
    Executes a GET request to the SEC API Extractor endpoint with retry logic for 'processing' status.
    """
    api_key = get_sec_api_key()
    params = {
        "url": filing_url,
        "item": item_code,
        "type": return_type,
        "token": api_key
    }

    max_retries = 4
    for attempt in range(max_retries):
        try:
            # Use SESSION.get directly here to handle the specific 'processing' retry logic
            response = SESSION.get(SEC_EXTRACTOR_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                content = response.text
                # Check for the specific "processing" response status
                if content.strip().lower() == "processing":
                    if attempt < max_retries - 1:
                        # As advised by docs, wait and retry
                        logging.info(f"Filing still processing for {item_code}. Retrying in 1.5 seconds...")
                        time.sleep(1.5)
                        continue
                    else:
                        logging.warning(f"Max retries reached for {filing_url} item {item_code}. Status: Processing.")
                        return None
                return content

            # If status is not 200, raise HTTPError
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            # Specific handling for common errors
            if e.response.status_code == 429:
                logging.warning("Rate limit hit (429) during Extractor API. Sleeping for 10 seconds and retrying...")
                time.sleep(10)
                if attempt < max_retries - 1:
                    continue
            # 404 often means the section doesn't exist (common in 8-K or older filings)
            elif e.response.status_code == 404:
                # logging.debug(f"Section {item_code} not found (404) in {filing_url}.")
                return None
            else:
                logging.error(f"HTTP Error extracting {item_code} from {filing_url}: {e.response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Connection Error during SEC Extractor request for {filing_url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff for connection errors
            else:
                return None
    return None