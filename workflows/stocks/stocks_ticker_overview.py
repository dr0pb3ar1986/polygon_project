# Import all our tools
from project_core import api_handler
from project_core import workflow_helpers


def fetch_and_save_all_stock_overviews():
    """
    Workflow to fetch all stock ticker overviews concurrently.
    This is now a simple wrapper around the generic helper function.
    """
    workflow_helpers.run_concurrent_overview_workflow(
        'stocks',
        api_handler.get_ticker_details,
        post_processor=workflow_helpers.format_stock_overview_data
    )