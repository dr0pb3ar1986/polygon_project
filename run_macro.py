# run_macro.py

# Import the required macro workflow modules
# Note: Ensure PyCharm has the project root marked as a Sources Root if imports fail
from workflows.macro_data import economic_indicators

def main():
    """
    This script runs the macro-related data workflows.
    """
    print("--- LAUNCHING ALL-MACRO DATA WORKFLOW ---")

    try:
        # Run the workflows in sequence
        print("\n--- Processing Economic Indicators (Rates, Inflation) ---")
        economic_indicators.fetch_and_save_economic_indicators()
        print("\n--- Economic Indicators Complete ---")

    except Exception as e:
        print(f"\n--- X An error occurred during the macro workflows: {e} ---")

    print("\n--- ALL-MACRO WORKFLOW FINISHED ---")

if __name__ == "__main__":
    main()