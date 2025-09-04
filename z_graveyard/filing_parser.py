# In project_core/filing_parser.py

import re
import json
import html
import unicodedata


def clean_text_for_ai(text):
    """
    A comprehensive cleaning function to prepare SEC filing text for Vertex AI.
    """
    if not text:
        return ""

    # 1. Normalization
    text = html.unescape(text)
    text = unicodedata.normalize('NFKC', text)

    # 2. Remove Noise and Artifacts
    text = re.sub(r'\b\w+[-_]*\w*\.(htm|html|xml)\b', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'Table of Contents', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'^.*?\.{5,}.*?[\dF-]+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'\bF\s*-\s*\d+\b', ' ', text)
    text = re.sub(r'-\s*\d+\s*-', ' ', text)
    text = re.sub(r'^\s*\d+\s*$', '', text, flags=re.MULTILINE)

    # 3. Whitespace Normalization
    text = re.sub(r'[ \t]+', ' ', text)

    # 4. Text Reflow (CRITICAL for NLP)
    text = re.sub(r'(\n\s*){2,}', '\n\n', text)
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)

    return text.strip()


def extract_text_from_soup(soup):
    """
    Extracts text while preserving basic structure for segmentation.
    """
    if soup is None: return ""
    # Use separator='\n' to respect block elements (like <p>, <div>)
    text = soup.get_text(separator='\n', strip=True)
    return text


# --- Define Robust Regex Patterns ---
# Pattern for 10-K/10-Q: Highly flexible.
# Allows indentation, optional PART headers, and different separators including dashes.
TEN_K_Q_PATTERN = re.compile(
    r'^\s{0,15}(?:PART\s+[IVX]+\s*[,.\-]?\s*)?ITEM\s*(\d{1,2}[A-Z]?)[\s\.\:\-–—]\s*',
    re.IGNORECASE | re.MULTILINE
)
# Pattern for 8-K: Handles "Item 1.01", "ITEM 8.01"
EIGHT_K_PATTERN = re.compile(
    r'^\s{0,15}ITEM\s*(\d\.\d{2})[\s\.\:\-–—]*\s*',
    re.IGNORECASE | re.MULTILINE
)


def find_document_start(full_text, form_type):
    """
    Heuristic to skip the Table of Contents (TOC) and find the start of the main content.
    """
    if '10-K' in form_type or '10-Q' in form_type:
        # Strategy: Look for the *second* occurrence of "Item 1" or "PART I".
        # The first is usually in the TOC.
        start_pattern = re.compile(r'^\s*(PART\s+I|Item\s+1)\b', re.IGNORECASE | re.MULTILINE)
        matches = list(start_pattern.finditer(full_text))

        if len(matches) > 1:
            # If the distance between the first two matches is small, it's likely a TOC.
            if matches[1].start() - matches[0].start() < 5000:
                return matches[1].start()
            return matches[0].start()
        elif len(matches) == 1:
            return matches[0].start()

    return 0  # Fallback for 8-K or if heuristics fail


def format_as_jsonl(sections, metadata):
    """
    Helper function to format the output as JSONL strings, preserving original schema.
    """
    jsonl_output_lines = []
    for section_name, text in sections.items():
        if not text: continue
        record = {
            "ticker": metadata.get('ticker'),
            "form_type": metadata.get('form_type'),
            "filing_date": metadata.get('filing_date'),
            "filing_timestamp": metadata.get('filing_timestamp'),
            "section": section_name,
            "text": text
        }
        jsonl_output_lines.append(json.dumps(record))

    return "\n".join(jsonl_output_lines)


def process_fallback(full_text, metadata):
    """
    Handles filings where segmentation fails. Cleans the text and stores it as a single block.
    """
    cleaned_text = clean_text_for_ai(full_text)

    # Final trim for fallback: Remove content after "SIGNATURES" if present
    end_match = re.search(r'\n\s*SIGNATURES\s*\n', cleaned_text, re.IGNORECASE)
    if end_match:
        cleaned_text = cleaned_text[:end_match.start()]

    sections = {"Full_Document_Text": cleaned_text.strip()}
    return format_as_jsonl(sections, metadata)


def segment_text(full_text, pattern):
    """
    Segments the text using the provided regex pattern.
    """
    matches = list(pattern.finditer(full_text))
    sections = {}

    for i in range(len(matches)):
        start_match = matches[i]
        item_number = start_match.group(1).strip().upper()
        section_key = f"Item_{item_number}"
        start_index = start_match.end()

        # Determine the end index (the start of the NEXT item or SIGNATURES)
        end_index = len(full_text)
        if i + 1 < len(matches):
            end_index = matches[i + 1].start()
        else:
            signature_match = re.search(r'\n\s*SIGNATURES\s*\n', full_text[start_index:], re.IGNORECASE)
            if signature_match:
                end_index = start_index + signature_match.start()

        raw_section_text = full_text[start_index:end_index]
        cleaned_text = clean_text_for_ai(raw_section_text)

        # Skip empty/short sections
        if cleaned_text and len(cleaned_text) > 50:
            if section_key in sections:
                # If a section is duplicated (e.g., TOC was missed), append the content.
                sections[section_key] += "\n\n[SECTION CONTINUED]\n\n" + cleaned_text
            else:
                sections[section_key] = cleaned_text

    return sections, len(matches)


def segment_and_process_filing(soup, metadata):
    """
    Central dispatcher function. Takes preprocessed soup, extracts text, segments, and formats.
    """
    full_text = extract_text_from_soup(soup)
    form_type = metadata.get('form_type', '').upper()

    start_pos = find_document_start(full_text, form_type)
    body_text = full_text[start_pos:]

    # Dispatch based on Form Type and set minimum section thresholds
    if '10-K' in form_type:
        pattern = TEN_K_Q_PATTERN
        min_sections = 4
    elif '10-Q' in form_type:
        pattern = TEN_K_Q_PATTERN
        min_sections = 3
    elif '8-K' in form_type:
        pattern = EIGHT_K_PATTERN
        min_sections = 1
    else:
        return process_fallback(body_text, metadata)  # Fallback for other types

    # Perform segmentation
    sections, match_count = segment_text(body_text, pattern)

    # Revert to fallback if segmentation finds too few sections
    if match_count < min_sections:
        print(
            f"  > Segmentation found only {match_count}/{min_sections} sections for {metadata.get('ticker')} {metadata.get('form_type')}. Using fallback.")
        return process_fallback(body_text, metadata)

    return format_as_jsonl(sections, metadata)