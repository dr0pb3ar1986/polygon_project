# Import all our tools
from project_core import workflow_helpers

def fetch_and_save_all_stock_relatives():
    """
    Workflow to fetch all stock ticker relatives concurrently.
    This is now a simple wrapper around the generic helper function.
    """
    workflow_helpers.run_concurrent_relatives_workflow()