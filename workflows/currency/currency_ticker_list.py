# Import our custom tools
from project_core import workflow_helpers


def fetch_and_save_all_currency_tickers():
    """
    Workflow to fetch all forex tickers.
    This is now a simple wrapper around the generic helper function.
    """
    endpoint = "/v3/reference/tickers"
    params = {'market': 'fx', 'limit': 1000}
    workflow_helpers.run_paginated_list_workflow('currency', endpoint, params)