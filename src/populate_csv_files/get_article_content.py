import pandas as pd
from bs4 import BeautifulSoup
import requests
from src.utils import root_directory, return_driver
from concurrent.futures import ThreadPoolExecutor
import os
import pdfkit
import logging
import re
import time
import markdown  # You may need to install this with `pip install markdown`

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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


def fetch_discourse_content_from_url(url, css_selector="div.post[itemprop='articleBody']"):
    """
    Fetch and neatly parse the content of an article from a URL using the specified CSS selector.
    The content is returned in Markdown format.

    Parameters:
    - url (str): The URL of the article.
    - css_selector (str): The CSS selector to locate the content container in the HTML.

    Returns:
    - str: The neatly parsed content of the article in Markdown format, or None if the content could not be fetched.
    """
    try:
        response = safe_request(url)  # Use the safe_request function to handle potential 429 errors.
        if response is None:
            return None

        response.encoding = 'utf-8'  # Force UTF-8 encoding
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')
        content_container = soup.select_one(css_selector)

        if not content_container:
            logging.warning(f"No content found for URL {url} with selector {css_selector}")
            return None

        markdown_content = ''.join(html_to_markdown(element) for element in content_container if element.name is not None)
        markdown_content = sanitize_mojibake(markdown_content)  # Clean up the content after parsing
        logging.info(f"Fetched content for URL {url}")
        return markdown_content
    except Exception as e:
        logging.error(f"Could not fetch content for URL {url}: {e}")
        return None




def fetch_discourse_titles(url):
    return fetch_discourse_content_from_url(url)


def fetch_content(row):
    url = getattr(row, 'article')

    # Define a mapping of URL patterns to functions
    url_patterns = {
        'ethresear.ch': fetch_discourse_content_from_url,
        'collective.flashbots.net': fetch_discourse_content_from_url,
        'lido.fi': fetch_discourse_content_from_url,
        'research.anoma': fetch_discourse_content_from_url,
        
        # 'frontier.tech': fetch_frontier_tech_titles,
        # 'vitalik.ca': fetch_vitalik_ca_titles,
        # 'writings.flashbots': fetch_flashbots_writings_titles,
        # 'medium.com': fetch_medium_titles,
        # 'blog.metrika': fetch_medium_titles,
        # 'mirror.xyz': fetch_mirror_titles,
        # 'iex.io': fetch_iex_titles,
        # 'paradigm.xyz': fetch_paradigm_titles,
        # 'hackmd.io': fetch_hackmd_titles,
        # 'jumpcrypto.com': fetch_jump_titles,
        # 'notion.site': fetch_notion_titles,  # Placeholder for fetch_notion_titles
        # 'notes.ethereum.org': fetch_notion_titles,  # Placeholder for fetch_notion_titles
        # 'succulent-throat-0ce.': fetch_notion_titles,  # Placeholder for fetch_notion_titles
        # 'propellerheads.xyz': fetch_propellerheads_titles,
        # 'a16z': fetch_a16z_titles,
        # 'blog.uniswap': None,  # Placeholder for fetch_uniswap_titles
        # 'osmosis.zone': fetch_osmosis_titles,
        # 'mechanism.org': fetch_mechanism_titles,
    }

    # TODO 2023-09-18: add substack support

    # Iterate through URL patterns and fetch contents
    for pattern, fetch_function in url_patterns.items():
        if pattern in url:
            if fetch_function:
                return fetch_function(url)
            else:
                return None

    return None  # Default case if no match is found


def sanitize_filename(title):
    """
    Sanitize the title to create a valid filename.
    Handles cases where the title might not be a string (e.g., NaN values).
    """
    if not isinstance(title, str):
        # Handle non-string titles, for example, replace NaNs with a placeholder
        title = "untitled"

    return "".join([c for c in title if c.isalpha() or c.isdigit() or c==' ']).rstrip() + ".pdf"


def fetch_article_contents_and_save_as_pdf(csv_filepath, output_dir, num_articles=None, overwrite=True):
    """
    Fetch the contents of articles and save each content as a PDF in the specified directory.

    Parameters:
    - csv_filepath (str): The file path of the input CSV file containing article URLs and referrers.
    - output_dir (str): The directory where the article PDFs should be saved.
    - num_articles (int, optional): Number of articles to process. If None, process all articles.
    """
    # Step 1: Read the CSV file
    df = pd.read_csv(csv_filepath)

    # If num_articles is specified, slice the DataFrame
    if num_articles is not None:
        df = df.head(num_articles)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Function to process each row
    def process_row(row):
        article_url = getattr(row, 'article')
        article_title = getattr(row, 'title')

        # Create a sanitized file name for the PDF from the article title
        pdf_filename = os.path.join(output_dir, sanitize_filename(article_title))

        # Check if PDF already exists
        if not os.path.exists(pdf_filename) or overwrite:
            content = fetch_content(row)
            if content:
                # Specify additional options for pdfkit to ensure UTF-8 encoding
                options = {
                    'encoding': "UTF-8",
                    'custom-header': [
                        ('Content-Encoding', 'utf-8'),
                    ],
                    'no-outline': None
                }
                pdfkit.from_string(markdown_to_html(content), pdf_filename, options=options)
                logging.info(f"Saved PDF for {article_url}")
            else:
                logging.warning(f"No content fetched for {article_url}")

    # Use ThreadPoolExecutor to process rows in parallel
    with ThreadPoolExecutor() as executor:
        list(executor.map(process_row, df.itertuples()))


def run():
    fetch_article_contents_and_save_as_pdf(
        f'{root_directory()}/data/links/articles_updated.csv',
        f'{root_directory()}/data/articles_pdf_download/',
        # num_articles=5
    )

run()