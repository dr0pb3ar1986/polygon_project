# Import our custom tools
from project_core import workflow_helpers


def fetch_and_save_all_options_tickers():
    """
    Workflow to fetch all options contract tickers.
    This is now a simple wrapper around the generic helper function.
    """
    endpoint = "/v3/reference/options/contracts"
    params = {'limit': 1000}
    workflow_helpers.run_paginated_list_workflow('options', endpoint, params)