
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# --- Global Configuration and Session for efficiency ---
SESSION = requests.Session()
BASE_URL = "https://api.polygon.io"
REQUEST_TIMEOUT = (5, 15)

# --- Module-level cache for the API key to avoid re-reading the file ---
_API_KEY = None


def get_api_key():
    """
    Loads the API key from the .env file, caching it for subsequent calls.
    """
    global _API_KEY
    if _API_KEY is None:
        load_dotenv()  # <-- SEE, THIS LINE USES THE IMPORT!
        _API_KEY = os.getenv("POLYGON_API_KEY")
        if not _API_KEY:
            print("Error: 'POLYGON_API_KEY' not found. Make sure it is set correctly in your .env file.")
            return None
    return _API_KEY

def _execute_session_get(url, params=None):
    """
    A private helper to execute a GET request and handle common exceptions.
    """
    try:
        response = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print(f"  > Request timed out for URL: {url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  > Error during API request for {url}: {e}")
        return None

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
        if data is None:
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

    if data is not None:
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
        print(f"  > Invalid date format for {ticker}. Expected YYYY-MM-DD.")
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

        if data is None:
            break
        results_on_page = data.get('results', [])
        if not results_on_page:
            break

        all_results.extend(results_on_page)
        print(f"  > Found {len(results_on_page)} results on this page. Total results: {len(all_results)}")

        if len(results_on_page) < limit:
            break
        last_timestamp = results_on_page[-1]['t']
        current_from = last_timestamp + 1
        page_count += 1
    return all_results

def get_trades_data(ticker, from_date, to_date, params=None):
    endpoint = f"/v3/trades/{ticker}"
    request_params = params.copy() if params else {}
    request_params['timestamp.gte'] = f"{from_date}T00:00:00Z"
    request_params['timestamp.lte'] = f"{to_date}T23:59:59Z"
    request_params['limit'] = 50000
    print(f"--- Fetching Trades for {ticker} from {from_date} to {to_date} ---")
    return get_paginated_data(endpoint, request_params)

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