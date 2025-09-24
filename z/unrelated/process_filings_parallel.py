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
            print(f"  [ERROR] PDF creation failed for {output_filepath.name}: {pisa_status.err}")
            return False
        return True
    except Exception as e:
        print(f"  [ERROR] An exception occurred during PDF creation for {output_filepath.name}: {e}")
        return False


def process_single_ticker(ticker_dir: Path, output_dir: Path, generate_pdf: bool):
    """Contains all the logic to process a single ticker's folder."""
    ticker = ticker_dir.name
    print(f"[{ticker}] Starting processing...")

    filings_data = {}
    jsonl_files = list(ticker_dir.glob('*.jsonl'))
    if not jsonl_files:
        print(f"  [{ticker}] No .jsonl files found. Skipping.")
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
                    print(f"  [{ticker}] Warning: Skipping malformed line in {jsonl_file.name}")

    if not filings_data:
        print(f"  [{ticker}] No valid data found. Skipping.")
        return

    # Sort filings by date
    sorted_filings = sorted(
        filings_data.values(),
        key=lambda f: f['metadata']['period_end_date'],
        reverse=True
    )

    # Generate the chosen output file
    if generate_pdf:
        # --- PDF Generation ---
        output_filepath = output_dir / f"{ticker}_filings_consolidated.pdf"
        html_template = """
        <html><head><style>
            @page {{ size: A4; margin: 1.5cm; }}
            h1, h2, h3 {{ font-family: sans-serif; color: #333; }}
            body {{ font-family: serif; }}
            .filing-divider {{ page-break-before: always; }}
            table {{ border-collapse: collapse; width: 100%; page-break-inside: auto; }}
            tr {{ page-break-inside: avoid; page-break-after: auto; }}
            th, td {{ border: 1px solid #dddddd; text-align: left; padding: 6px; font-size: 9px; }}
            th {{ background-color: #f2f2f2; }}
        </style></head><body>{content}</body></html>
        """

        content_html = ""
        for i, filing in enumerate(sorted_filings):
            meta = filing['metadata']
            if i > 0:
                content_html += '<div class="filing-divider"></div>'

            content_html += f"<h2>COMPANY: {meta.get('company_name')} ({meta.get('ticker')})</h2>"
            content_html += f"<h3>FORM TYPE: {meta.get('form_type')} | PERIOD END DATE: {meta.get('period_end_date')}</h3>"
            content_html += f"<p><b>Accession Number:</b> {meta.get('accession_number')}</p><hr>"
            content_html += "\n".join(filing['sections_html'])

        # **FIX**: This is the variable that was previously unresolved. It's now correctly defined.
        full_html_content = html_template.format(content=content_html)

        print(f"  [{ticker}] Generating PDF...")
        if create_pdf_from_html(full_html_content, output_filepath):
            print(f"  [{ticker}] Successfully created PDF.")
        else:
            print(f"  [{ticker}] FAILED to create PDF.")
    else:
        # --- Formatted TXT Generation ---
        output_filepath = output_dir / f"{ticker}_filings_consolidated.txt"
        with open(output_filepath, 'w', encoding='utf-8') as outfile:
            for filing in sorted_filings:
                meta = filing['metadata']

                outfile.write("=" * 80 + "\n")
                outfile.write(f"COMPANY: {meta.get('company_name')} ({meta.get('ticker')})\n")
                outfile.write(f"FORM TYPE: {meta.get('form_type')}\n")
                outfile.write(f"PERIOD END DATE: {meta.get('period_end_date')}\n")
                outfile.write("=" * 80 + "\n\n")

                # **FIX**: This variable also needs to be defined within the loop for TXT generation.
                full_html_content_for_filing = "\n".join(filing['sections_html'])
                soup = BeautifulSoup(full_html_content_for_filing, 'lxml')

                for element in soup.find_all(['h1', 'p', 'table', 'div']):
                    if element.name == 'table':
                        try:
                            df_list = pd.read_html(str(element), flavor='lxml')
                            for df in df_list:
                                outfile.write(df.to_markdown(index=False, tablefmt="grid"))
                                outfile.write("\n\n")
                        except Exception:
                            outfile.write(element.get_text(separator=' ', strip=True))
                            outfile.write("\n\n")
                    else:
                        text = element.get_text(strip=True)
                        if text:  # Only write if there's text content
                            outfile.write(text + "\n\n")

        print(f"  [{ticker}] Successfully created formatted TXT file.")


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

    ticker_dirs = [d for d in parent_path.iterdir() if d.is_dir() and d.name != "cleaned"]
    tasks = [(ticker_dir, output_dir, generate_pdf) for ticker_dir in ticker_dirs]

    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(process_single_ticker, tasks)

    print("\nAll processing complete.")


if __name__ == "__main__":
    # Set your parent directory here. The script can be run from anywhere.
    PARENT_DIR = r"Y:\Polygon\data_output\stocks\filings"

    # Set to True for PDF, False for TXT.
    GENERATE_PDF = True

    # Set the number of parallel worker processes.
    NUM_WORKERS = 10
    # For optimal performance, a good general rule is os.cpu_count() - 1
    # You could use: NUM_WORKERS = max(1, os.cpu_count() - 1)

    run_parallel_processing(
        parent_directory=PARENT_DIR,
        num_workers=NUM_WORKERS,
        generate_pdf=GENERATE_PDF
    )