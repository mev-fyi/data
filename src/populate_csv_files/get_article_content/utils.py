import logging
import re
import time

import markdown
import requests
from datetime import datetime



def safe_request(url, max_retries=5, backoff_factor=0.3):
    """
    Safe request handling with retries and exponential backoff.

    Parameters:
    - url (str): The URL to fetch.
    - max_retries (int): Maximum number of retries.
    - backoff_factor (float): Factor by which to increase delay between retries.

    Returns:
    - Response object or None if all retries fail.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                sleep_duration = backoff_factor * (2 ** attempt)
                logging.warning(f"Rate limit hit. Retrying in {sleep_duration} seconds.")
                time.sleep(sleep_duration)
            else:
                raise
    logging.error(f"Failed to fetch URL {url} after {max_retries} retries.")
    return None


def clean_up_mojibake(text):
    """
    Replace common mojibake occurrences with the correct character.
    """
    replacements = {
        'â€™': "'",
        'â€œ': '"',
        'â€�': '"',
        'â€”': '—',
        'â€“': '-',
        'â€¦': '...',
        'â€˜': '‘',
        'â€™': '’',
        'â€¢': '•',
        'Ã ': 'à',
        'Ã©': 'é',
        'Ã¨': 'è',
        'Ã¯': 'ï',
        'Ã´': 'ô',
        'Ã¶': 'ö',
        'Ã»': 'û',
        'Ã§': 'ç',
        'Ã¤': 'ä',
        'Ã«': 'ë',
        'Ã¬': 'ì',
        'Ã­': 'í',
        'Ã¢': 'â',
        'Ã¼': 'ü',
        'Ã±': 'ñ',
        'Ã¡': 'á',
        'Ãº': 'ú',
        'Ã£': 'ã',
        'Ãµ': 'õ',
        'Ã¦': 'æ',
        'Ã°': 'ð',
        'Ã¿': 'ÿ',
        'Ã½': 'ý',
        'Ã¾': 'þ',
        'Ã': 'í',
        'â‚¬': '€',
        'â€™s': "'s",
        'doesnâ€™t': "doesn't",
        'donâ€™t': "don't",
        'canâ€™t': "can't",
        'isnâ€™t': "isn't",
        'arenâ€™t': "aren't",
        'werenâ€™t': "weren't",
        'havenâ€™t': "haven't",
        'hasnâ€™t': "hasn't",
        'didnâ€™t': "didn't",
        'wouldnâ€™t': "wouldn't",
        'shouldnâ€™t': "shouldn't",
        'couldnâ€™t': "couldn't",
        'â€™ll': "'ll",
        'â€™re': "'re",
        'â€™ve': "'ve",
        'â€™d': "'d",
        'â€™m': "'m",
        # Add more replacements as needed
    }
    for wrong, right in replacements.items():
        text = text.replace(wrong, right)
    return text


def sanitize_mojibake(text):
    # Regex patterns for common mojibake sequences
    mojibake_patterns = {
        re.compile(r'â€™'): "'",
        re.compile(r'â€œ'): '“',
        re.compile(r'â€'): '”',
        re.compile(r'â€”'): '—',
        re.compile(r'â€“'): '–',
        # Add more patterns as needed
    }

    # Replace each mojibake pattern with the correct character
    for pattern, replacement in mojibake_patterns.items():
        text = pattern.sub(replacement, text)

    return text


def html_to_markdown(element):
    """
    Convert an HTML element to its Markdown representation.
    """
    # Retrieve text from the HTML element directly, without cleaning up mojibake
    tag_name = element.name
    text = element.get_text()

    if tag_name == 'p':
        return text.strip() + '\n\n'
    elif tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        header_level = int(tag_name[1])
        return '#' * header_level + ' ' + text.strip() + '\n\n'
    elif tag_name == 'ul':
        return '\n'.join([f"* {li.get_text().strip()}" for li in element.find_all('li')]) + '\n\n'
    elif tag_name == 'ol':
        return '\n'.join([f"1. {li.get_text().strip()}" for li in element.find_all('li')]) + '\n\n'
    # Add more HTML to Markdown conversions here as needed
    else:
        return text.strip() + '\n\n'


def markdown_to_html(markdown_content):
    """
    Convert Markdown content to HTML with UTF-8 encoding specified.
    """
    # Add the UTF-8 meta charset tag
    html_content = '<meta charset="UTF-8">' + markdown.markdown(markdown_content)
    return html_content


def sanitize_filename(title):
    """
    Sanitize the title to create a valid filename.
    Handles cases where the title might not be a string (e.g., NaN values).
    """
    if not isinstance(title, str):
        # Handle non-string titles, for example, replace NaNs with a placeholder
        title = "untitled"

    return "".join([c for c in title if c.isalpha() or c.isdigit() or c==' ']).rstrip() + ".pdf"


def convert_date_format(date_str):
    try:
        # Try to parse the date with the full format including the year
        date_obj = datetime.strptime(date_str, '%b %d, %Y')
    except ValueError:
        try:
            # If the year is missing, parse without the year and use the current year
            date_obj = datetime.strptime(date_str, '%b %d').replace(year=datetime.now().year)
        except ValueError:
            # Return None or raise an error if the date format is incorrect
            return None
    # Format the date to yyyy-mm-dd
    return date_obj.strftime('%Y-%m-%d')


def convert_frontier_tech_date_format(date_str):
    # Check if date_str is not None
    if date_str is None:
        return None

    # Define a list of possible date formats to try
    date_formats = ['%d %B %Y', '%B %Y', '%d %b %Y']

    # Attempt to parse the date using different formats
    for date_format in date_formats:
        try:
            date_obj = datetime.strptime(date_str, date_format)
            # If the parsing is successful, return the formatted date
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            # Try to parse with the current year if only month and day are given
            if date_format == '%d %b':
                try:
                    # Assume current year if only month and day are provided
                    current_year = datetime.now().year
                    date_str_with_year = f"{date_str} {current_year}"
                    date_obj = datetime.strptime(date_str_with_year, '%d %b %Y')
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass
            continue  # Try the next format if parsing fails

    # If none of the formats match, return None or raise an exception
    return None  # Return None if the date cannot be parsed with any of the formats

