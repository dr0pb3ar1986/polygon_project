import re

def _is_potential_heading(line):
    """
    Identifies if a line is a potential heading.
    - All caps (with some tolerance for short lines)
    - Starts with "Item" or "ITEM"
    - Common SEC filing section titles
    """
    line = line.strip()
    if not line:
        return False

    # Rule 1: Starts with "Item" (case-insensitive)
    if line.lower().startswith('item'):
        return True

    # Rule 3: Common section titles (can be expanded)
    common_titles = [
        "part i", "part ii", "part iii", "part iv",
        "financial statements", "notes to consolidated financial statements",
        "business", "risk factors", "management's discussion and analysis"
    ]
    if line.lower() in common_titles:
        return True

    return False

def parse_to_markdown(text_content):
    """
    Parses raw SEC filing text and formats it into Markdown.
    """
    # Normalize newlines
    text_content = text_content.replace('\\n', '\n')
    lines = text_content.split('\n')

    formatted_lines = []

    # Add a main title for the document
    if lines:
        # Heuristic: Find the first meaningful line to use as a title
        for line in lines[:20]:
            if line.strip() and "form type" in line.lower():
                formatted_lines.append(f"# {line.strip()}")
                break
        else:
            formatted_lines.append("# SEC Filing")

    formatted_lines.append("\n---")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if _is_potential_heading(line):
            # Use H2 for major sections and H3 for sub-items
            if line.lower().startswith('part'):
                formatted_lines.append(f"\n## {line}\n")
            else:
                formatted_lines.append(f"\n### {line}\n")
        else:
            # Simple paragraph formatting
            formatted_lines.append(line)

    # Join lines and handle excessive newlines
    full_text = '\n'.join(formatted_lines)
    # Reduce more than 2 consecutive newlines to just 2
    return re.sub(r'(\n){3,}', '\n\n', full_text)
