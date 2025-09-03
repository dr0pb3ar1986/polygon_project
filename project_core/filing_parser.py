import re


def clean_text_for_ai(text):
    """
    A more advanced cleaning function to prepare text for an AI model.
    It removes tables of contents, page numbers, and other artifacts.
    """
    # Remove table of contents sections
    text = re.sub(r'TABLE OF CONTENTS.*?(?=Item\s+[1-9])', '', text, flags=re.IGNORECASE | re.DOTALL)

    # Remove lines that are just page numbers
    text = re.sub(r'^\s*\d+\s*$', '', text, flags=re.MULTILINE)

    # Remove lines that appear to be part of a table of contents (e.g., "Business..... 1")
    text = re.sub(r'^.*\.+\s*\d+\s*$', '', text, flags=re.MULTILINE)

    # Normalize whitespace and clean up the result
    text = re.sub(r'\n\s*\n', '\n', text)
    return text.strip()


def parse_to_structured_text(raw_text, metadata):
    """
    Parses raw SEC filing text into a structured, templated, and cleaned
    text file ideal for AI consumption.
    """
    text_content = raw_text.replace('\\n', '\n').replace('&nbsp;', ' ')

    sections_to_extract = [
        "Item 1. Business", "Item 1A. Risk Factors", "Item 1B. Unresolved Staff Comments",
        "Item 2. Properties", "Item 3. Legal Proceedings", "Item 4. Mine Safety Disclosures",
        "Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations",
        "Item 7A. Quantitative and Qualitative Disclosures About Market Risk",
        "Item 8. Financial Statements and Supplementary Data", "Item 9A. Controls and Procedures",
        "Item 9B. Other Information",
    ]

    output_lines = [
        "[START FILING METADATA]",
        f"Ticker: {metadata.get('ticker', 'N/A')}",
        f"Form Type: {metadata.get('form_type', 'N/A')}",
        f"Filing Date: {metadata.get('filing_date', 'N/A')}",
        f"Filing Timestamp: {metadata.get('filing_timestamp', 'N/A')}",  # New Timestamp
        "[END FILING METADATA]",
        "\n---"
    ]

    for i, section_name in enumerate(sections_to_extract):
        item_pattern = re.escape(section_name.split('.')[0])
        start_regex = re.compile(rf'^\s*{item_pattern}', re.IGNORECASE | re.MULTILINE)

        start_match = start_regex.search(text_content)
        if not start_match:
            continue

        end_match = None
        if i + 1 < len(sections_to_extract):
            next_item_pattern = re.escape(sections_to_extract[i + 1].split('.')[0])
            end_regex = re.compile(rf'^\s*{next_item_pattern}', re.IGNORECASE | re.MULTILINE)
            end_match = end_regex.search(text_content, start_match.end())

        start_pos = start_match.start()
        end_pos = end_match.start() if end_match else len(text_content)
        section_content = text_content[start_pos:end_pos]

        # Apply the new advanced cleaning function
        cleaned_section = clean_text_for_ai(section_content)

        if cleaned_section:  # Only add sections that have content after cleaning
            output_lines.append(f"\n[START SECTION: {section_name}]")
            output_lines.append(cleaned_section)
            output_lines.append(f"[END SECTION: {section_name}]")
            output_lines.append("\n---")

    return "\n".join(output_lines)