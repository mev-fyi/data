import logging
import re
import time

import markdown
import requests
from datetime import datetime
from urllib.parse import urlparse

from src.utils import root_directory, return_driver


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
    Convert an HTML element to its Markdown representation, keeping URLs.
    """
    tag_name = element.name

    if tag_name == 'p':
        return ''.join(html_to_markdown(child) for child in element.contents).strip() + '\n\n'
    elif tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        header_level = int(tag_name[1])
        return '#' * header_level + ' ' + ''.join(html_to_markdown(child) for child in element.contents).strip() + '\n\n'
    elif tag_name == 'ul':
        return '\n'.join([f"* {html_to_markdown(li)}" for li in element.find_all('li')]).strip() + '\n\n'
    elif tag_name == 'ol':
        return '\n'.join([f"1. {html_to_markdown(li)}" for li in element.find_all('li')]).strip() + '\n\n'
    elif tag_name == 'a':
        url = element.get('href', '')
        text = ''.join(html_to_markdown(child) for child in element.contents)
        return f"[{text}]({url})"
    elif tag_name is None:  # Handle NavigableString elements
        return element.string
    else:
        return ''.join(html_to_markdown(child) for child in element.contents).strip() + '\n\n'


def html_to_markdown_a16z(element):
    """
    Convert an HTML element to its Markdown representation, keeping URLs.
    """
    from bs4 import NavigableString
    if isinstance(element, NavigableString):
        return str(element)

    tag_name = element.name

    if tag_name == 'p':
        return ' '.join(html_to_markdown_a16z(child) for child in element.contents).strip() + '\n\n'
    elif tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        header_level = int(tag_name[1])
        return '#' * header_level + ' ' + ' '.join(html_to_markdown_a16z(child) for child in element.contents).strip() + '\n\n'
    elif tag_name == 'ul':
        return '\n'.join([f"* {html_to_markdown_a16z(li)}" for li in element.find_all('li')]).strip() + '\n\n'
    elif tag_name == 'ol':
        return '\n'.join([f"1. {html_to_markdown_a16z(li)}" for li in element.find_all('li')]).strip() + '\n\n'
    elif tag_name == 'a':
        url = element.get('href', '')
        text = ' '.join(html_to_markdown_a16z(child) for child in element.contents).strip()
        return f"[{text}]({url})"
    elif tag_name == 'span':
        return ''.join(html_to_markdown_a16z(child) for child in element.contents)
    else:
        return ' '.join(html_to_markdown_a16z(child) for child in element.contents).strip()

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


def convert_mirror_date_format(date_str):
    try:
        # Remove the suffixes from the day part
        date_str = re.sub(r'(\d+)(st|nd|rd|th),', r'\1,', date_str)

        # Parse the date with the full format including the year
        date_obj = datetime.strptime(date_str, '%B %d, %Y')
    except ValueError:
        # Return None or raise an error if the date format is incorrect
        return None

    # Format the date to yyyy-mm-dd
    return date_obj.strftime('%Y-%m-%d')


import pandas as pd
from pathlib import Path


# Define the function to get the unmatched PDFs.
def get_unmatched_pdfs(papers_directory: Path, details_csv: Path):
    # Read the dataframe from CSV
    paper_details_df = pd.read_csv(details_csv)

    # Extract the list of titles from the dataframe
    titles_from_df = set(paper_details_df['title'])

    # Get the list of PDF filenames from the directory
    pdf_filenames = {file.stem for file in papers_directory.glob('*.pdf')}

    # Find the titles that are in the dataframe but not in the directory
    unmatched_titles = titles_from_df - pdf_filenames

    # Filter the dataframe to only include rows with titles that are not matched
    unmatched_papers_df = paper_details_df[paper_details_df['title'].isin(unmatched_titles)]

    return unmatched_papers_df


def extract_unique_domains_from_dataframe(dataframe, link_field='pdf_link'):
    # Extract the 'pdf_link' column
    pdf_links = dataframe[link_field]

    # Extract the domains from the URLs
    domains = pdf_links.apply(lambda x: urlparse(x).netloc).unique()

    return set(domains)


if __name__ == '__main__':
    # Assuming the root_directory() function is defined elsewhere and returns the correct path.
    # For demonstration, let's assume the root directory is the current working directory.
    root_dir = root_directory()

    # Paths are constructed based on the assumed root directory function.
    # papers_directory = Path(f'{root_dir}/data/papers_pdf_downloads')
    # details_csv = Path(f'{root_dir}/data/paper_details.csv')

    # Call the function and print the result.
    # Note: This will only work if the actual data exists in the specified paths.
    # unmatched_pdfs_df = get_unmatched_pdfs(papers_directory, details_csv)
    # unique_domains = extract_unique_domains_from_dataframe(unmatched_pdfs_df)
    # print(unique_domains)

    # Paths are constructed based on the assumed root directory function.
    articles_directory = Path(f'{root_dir}/data/articles_pdf_download')
    article_details_csv = Path(f'{root_dir}/data/links/articles_updated.csv')

    # Call the function and print the result.
    # Note: This will only work if the actual data exists in the specified paths.
    unmatched_pdfs_articles_df = get_unmatched_pdfs(articles_directory, article_details_csv)
    unique_domains = extract_unique_domains_from_dataframe(unmatched_pdfs_articles_df, link_field='article')
    print(unique_domains)

    # Since we cannot execute with actual file paths in this environment, the above lines are commented out.
    # They are provided for reference to show how the function should be called in a real scenario.
