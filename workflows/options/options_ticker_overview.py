# Import our custom tools
from project_core import api_handler
from project_core import workflow_helpers


def fetch_and_save_all_options_overviews():
    """
    Workflow to fetch all options ticker overviews concurrently.
    This is now a simple wrapper around the generic helper function.
    """
    workflow_helpers.run_concurrent_overview_workflow('options', api_handler.get_option_contract_details)