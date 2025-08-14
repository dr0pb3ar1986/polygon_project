# Import all our tools
from project_core import api_handler
from project_core import workflow_helpers


def fetch_and_save_all_indices_overviews():
    """
    Workflow to fetch all index ticker overviews concurrently.
    This is now a simple wrapper around the generic helper function.
    """
    workflow_helpers.run_concurrent_overview_workflow('indices', api_handler.get_ticker_details)