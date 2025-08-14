# Import all our tools, including the now-complete file_manager
from project_core import workflow_helpers


def fetch_and_save_all_indices_tickers():
    """
    Workflow to fetch all index tickers.
    This is now a simple wrapper around the generic helper function.
    """
    endpoint = "/v3/reference/tickers"
    params = {'market': 'indices', 'limit': 1000}
    workflow_helpers.run_paginated_list_workflow('indices', endpoint, params)