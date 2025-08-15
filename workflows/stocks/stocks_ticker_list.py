# Import all our tools
from project_core import workflow_helpers

def fetch_and_save_all_stock_tickers():
    """
    Workflow to fetch all stock tickers.
    This is now a simple wrapper around the generic helper function.
    """
    endpoint = "/v3/reference/tickers"
    params = {'market': 'stocks', 'active': 'true', 'limit': 1000}
    workflow_helpers.run_paginated_list_workflow('stocks', endpoint, params)


# This allows the file to be run directly for testing or ad-hoc execution.
if __name__ == "__main__":
    fetch_and_save_all_stock_tickers()