"""
UTF Sanitization utilities
Cleans and sanitizes text data to prevent encoding issues
"""

import unicodedata
import re


def sanitize_text(text: str, replacement: str = '') -> str:
    """
    Sanitize text by removing or replacing problematic UTF characters

    Args:
        text: Input text to sanitize
        replacement: String to replace problematic characters with

    Returns:
        Sanitized text safe for CSV and storage
    """
    if not text:
        return ''

    # Normalize unicode (NFC - Canonical Decomposition followed by Canonical Composition)
    text = unicodedata.normalize('NFC', text)

    # Remove null bytes
    text = text.replace('\x00', replacement)

    # Remove control characters except newline, carriage return, tab
    text = ''.join(char for char in text if unicodedata.category(char)[0] != 'C' or char in '\n\r\t')

    # Remove zero-width characters
    zero_width_chars = [
        '\u200b',  # Zero width space
        '\u200c',  # Zero width non-joiner
        '\u200d',  # Zero width joiner
        '\ufeff',  # Zero width no-break space (BOM)
    ]
    for char in zero_width_chars:
        text = text.replace(char, replacement)

    # Replace multiple spaces/tabs/newlines with single
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'\r+', '\r', text)

    # Trim whitespace
    text = text.strip()

    return text


def sanitize_for_csv(text: str) -> str:
    """
    Sanitize text specifically for CSV export
    Escapes quotes and handles newlines

    Args:
        text: Input text

    Returns:
        CSV-safe text
    """
    if not text:
        return ''

    # First sanitize UTF issues
    text = sanitize_text(text)

    # Extract URLs from <a href> tags and keep them
    text = re.sub(r'<a\s+href=["\']([^"\']+)["\'][^>]*>([^<]*)</a>', r'\2 (\1)', text)

    # Remove all other HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Escape double quotes by doubling them (CSV standard)
    text = text.replace('"', '""')

    # Replace newlines with space or keep them (depending on preference)
    # For CSV, it's safer to replace with space
    text = text.replace('\n', ' ').replace('\r', ' ')

    # Remove multiple spaces
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def ensure_utf8(text: str, errors: str = 'ignore') -> str:
    """
    Ensure text is valid UTF-8 by encoding and decoding

    Args:
        text: Input text
        errors: Error handling ('ignore', 'replace', 'strict')

    Returns:
        Valid UTF-8 text
    """
    if not text:
        return ''

    try:
        # Encode to UTF-8 bytes, then decode back
        # This removes characters that can't be encoded in UTF-8
        return text.encode('utf-8', errors=errors).decode('utf-8')
    except Exception:
        return ''


def sanitize_dict(data: dict, fields_to_sanitize: list = None) -> dict:
    """
    Sanitize all string values in a dictionary

    Args:
        data: Dictionary with data
        fields_to_sanitize: List of field names to sanitize (None = all strings)

    Returns:
        Dictionary with sanitized values
    """
    sanitized = {}

    for key, value in data.items():
        if fields_to_sanitize and key not in fields_to_sanitize:
            sanitized[key] = value
        elif isinstance(value, str):
            sanitized[key] = sanitize_for_csv(value)
        elif isinstance(value, (list, tuple)):
            sanitized[key] = [sanitize_text(str(v)) if isinstance(v, str) else v for v in value]
        else:
            sanitized[key] = value

    return sanitized


def remove_emojis(text: str, replacement: str = '') -> str:
    """
    Remove emoji characters from text

    Args:
        text: Input text
        replacement: String to replace emojis with

    Returns:
        Text without emojis
    """
    if not text:
        return ''

    # Remove emojis using Unicode ranges
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001F900-\U0001F9FF"  # supplemental symbols
        "\U0001FA00-\U0001FA6F"  # extended symbols
        "]+",
        flags=re.UNICODE
    )

    return emoji_pattern.sub(replacement, text)


def sanitize_filename(filename: str) -> str:
    """
    Sanitize string to be safe for use as filename

    Args:
        filename: Proposed filename

    Returns:
        Safe filename
    """
    if not filename:
        return 'unnamed'

    # Remove invalid filename characters
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)

    # Remove leading/trailing spaces and dots
    filename = filename.strip('. ')

    # Limit length
    if len(filename) > 255:
        filename = filename[:255]

    return filename or 'unnamed'
