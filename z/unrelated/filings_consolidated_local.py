import os
import json
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from xhtml2pdf import pisa
import multiprocessing


def create_pdf_from_html(html_content: str, output_filepath: Path):
    """Converts a string of HTML content into a PDF file."""
    try:
        with open(output_filepath, "w+b") as result_file:
            pisa_status = pisa.CreatePDF(html_content, dest=result_file)
        if pisa_status.err:
            return False
        return True
    except Exception as e:
        print(f"Error during PDF creation for {output_filepath.name}: {e}")
        return False


def process_single_ticker(ticker_dir: Path, output_dir: Path, generate_pdf: bool):
    """Contains all the logic to process a single ticker's folder."""
    ticker = ticker_dir.name
    print(f"[{ticker}] Starting processing...")

    filings_data = {}
    jsonl_files = list(ticker_dir.glob('*.jsonl'))
    if not jsonl_files:
        print(f"[{ticker}] No .jsonl files found. Skipping.")
        return

    # Aggregate data for the ticker
    for jsonl_file in jsonl_files:
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    accession_number = data.get('accession_number')
                    if not accession_number: continue

                    if accession_number not in filings_data:
                        filings_data[accession_number] = {
                            'metadata': data,
                            'sections_html': []
                        }

                    filings_data[accession_number]['sections_html'].append(
                        f"<h1>Section: {data.get('section_id', 'Unnamed Section')}</h1>\n{data.get('text', '')}"
                    )
                except json.JSONDecodeError:
                    print(f"[{ticker}] Warning: Skipping malformed line in {jsonl_file.name}")

    if not filings_data:
        print(f"[{ticker}] No valid data found. Skipping.")
        return

    # Sort filings by date
    sorted_filings = sorted(
        filings_data.values(),
        key=lambda f: f['metadata']['period_end_date'],
        reverse=True
    )

    # Generate the chosen output file
    if generate_pdf:
        output_filepath = output_dir / f"{ticker}_filings_consolidated.pdf"
        html_template = "..."  # Same template as before
        # ... (rest of PDF generation logic is identical to previous script)
        if create_pdf_from_html(full_html_content, output_filepath):
            print(f"[{ticker}] Successfully created PDF.")
        else:
            print(f"[{ticker}] FAILED to create PDF.")
    else:
        output_filepath = output_dir / f"{ticker}_filings_consolidated.txt"
        # ... (rest of TXT generation logic is identical to previous script)
        print(f"[{ticker}] Successfully created formatted TXT file.")


# --- The main function that sets up and runs the multiprocessing pool ---
def run_parallel_processing(parent_directory: str, num_workers: int, generate_pdf: bool):
    """
    Finds all ticker directories and distributes the processing across a pool of workers.
    """
    parent_path = Path(parent_directory)
    if not parent_path.is_dir():
        print(f"Error: Directory does not exist: {parent_directory}")
        return

    output_dir = parent_path / "cleaned"
    output_dir.mkdir(exist_ok=True)
    print(f"Output will be saved to: {output_dir}")
    print(f"Starting processing with {num_workers} workers...\n")

    # Find all ticker directories to process
    ticker_dirs = [d for d in parent_path.iterdir() if d.is_dir() and d.name != "cleaned"]

    # Prepare the arguments for each task
    tasks = [(ticker_dir, output_dir, generate_pdf) for ticker_dir in ticker_dirs]

    # Create a pool of worker processes and map tasks to them
    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(process_single_ticker, tasks)

    print("\nAll processing complete.")


if __name__ == "__main__":
    # --- IMPORTANT ---
    # Set your parent directory here.
    PARENT_DIR = r"Y:\Polygon\data_output\stocks\filings"

    # --- CHOOSE YOUR OUTPUT ---
    # Set to True for PDF, False for TXT.
    GENERATE_PDF = True

    # --- SET NUMBER OF WORKERS ---
    # As requested, set to 5.
    # For optimal performance, a good general rule is os.cpu_count() - 1
    NUM_WORKERS = 5

    # To use a dynamic number based on your machine, you could use:
    # NUM_WORKERS = max(1, os.cpu_count() - 1)

    run_parallel_processing(
        parent_directory=PARENT_DIR,
        num_workers=NUM_WORKERS,
        generate_pdf=GENERATE_PDF
    )