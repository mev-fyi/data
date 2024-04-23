import logging
import re
import time

import markdown
import requests
from datetime import datetime
from urllib.parse import urlparse, urljoin

from src.utils import root_directory, return_driver
from bs4 import NavigableString


def safe_request(url, max_retries=10, backoff_factor=1):
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


def html_to_markdown_docs(element, base_url):
    """
    Convert an HTML element to its Markdown representation, ensuring link texts do not contain unnecessary
    line breaks or excessive spaces, and appending the base URL to relative links.
    This version ensures proper separation between code blocks and subsequent content,
    and handles specific cases of Markdown headings followed by a newline.
    """
    tag_name = element.name
    classes = element.get('class', []) if not isinstance(element, NavigableString) else ''  # Gets the class list of the current element

    # Handle NavigableString without additional processing
    if isinstance(element, NavigableString):
        return str(element).strip()

    # Check if the current element is a code block by its class name and process it
    if 'vocs_CodeBlock' in classes or any(cls.startswith('group/codeblock') for cls in classes):
        return process_code_block(element, base_url) + '\n'  # Ensure there is a newline after a code block

    # Recursively process child elements
    children_text = ''.join(html_to_markdown_docs(child, base_url) for child in element.contents).strip()

    # Process different elements according to their tags
    processed_content = process_tag_based_content(tag_name, children_text, base_url, element) + '\n'  # Ensure there is a newline after each processed tag

    # Specific handling for headings with a newline
    processed_content = processed_content.replace('# \n', '# ')

    return processed_content


def process_code_block(element, base_url):
    """
    Handles processing of code block elements, ensuring proper Markdown formatting with triple backticks.
    Identifies individual lines of code within <span> elements and handles them correctly.
    This version ensures proper formatting and separation of code blocks.
    """
    code_lines = []

    def process_code_line(child):
        # If the child is a span, it might represent a single line of code.
        if child.name == 'span':
            line = ''.join(str(string).strip() for string in child.stripped_strings)
            code_lines.append(line)
        elif isinstance(child, NavigableString):
            stripped_line = str(child).strip()
            if stripped_line:  # Avoid adding empty lines
                code_lines.append(stripped_line)
        else:
            # For other elements within the code block, recursively process its children
            for inner_child in child.children:
                process_code_line(inner_child)

    for child in element.children:
        process_code_line(child)

    code_text = '\n'.join(code_lines)
    return f"```\n\n{code_text}\n\n```\n"  # Add a newline for separation

def process_tag_based_content(tag_name, children_text, base_url, element):
    """
    Processes HTML tags to their Markdown equivalent, handling links, headers, lists, and more.
    This version ensures proper separation and formatting of subsequent content.
    """
    if tag_name == 'p':
        return children_text + '\n\n'
    elif tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        header_level = int(tag_name[1])
        # Ensure there is a newline before headers if they follow a code block or another header
        return f"\n{'#' * header_level} {children_text}\n\n"
    elif tag_name == 'ul':
        items = [item for item in children_text.split('\n') if item.strip()]
        return '\n'.join(f"* {item}" for item in items).strip() + '\n\n'
    elif tag_name == 'ol':
        items = [item for item in children_text.split('\n') if item.strip()]
        return '\n'.join(f"{index + 1}. {item}" for index, item in enumerate(items)).strip() + '\n\n'
    elif tag_name == 'li':
        return children_text.strip() + '\n'
    elif tag_name == 'a':
        href = element.get('href', '').strip()
        url = urljoin(base_url, href) if not href.startswith('http') else href
        text = children_text.replace('\n', ' ').replace('  ', ' ').replace('/$', '').replace('$$', '').replace('$', '')
        return f"[{text}]({url})"
    else:
        return children_text.replace('/$', '').replace('$$', '').replace('$', '')


def html_to_markdown_docs_chainlink(element, base_url):
    """
    Convert an HTML element to its Markdown representation, ensuring link texts do not contain unnecessary
    line breaks or excessive spaces, and appending the base URL to relative links. Ignores non-text HTML tags like 'style', 'script', and 'comment'.
    """
    tag_name = element.name
    from bs4 import NavigableString, Comment
    if isinstance(element, Comment):
        # Ignore HTML comments
        return ''

    if tag_name in ['style', 'script']:
        # Skip style and script tags
        return ''

    if isinstance(element, NavigableString):
        return str(element).strip()

    children_text = ''.join(html_to_markdown_docs_chainlink(child, base_url) for child in element.contents).strip()

    # Handling different tags according to Markdown syntax
    if tag_name == 'p':
        return children_text + '\n\n'
    elif tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        header_level = int(tag_name[1])
        return '#' * header_level + ' ' + children_text + '\n\n'
    elif tag_name == 'ul':
        return '\n'.join(f"* {html_to_markdown_docs_chainlink(li, base_url)}" for li in element.find_all('li', recursive=False)).strip() + '\n\n'
    elif tag_name == 'ol':
        return '\n'.join(f"{index + 1}. {html_to_markdown_docs_chainlink(li, base_url)}" for index, li in enumerate(element.find_all('li', recursive=False))).strip() + '\n\n'
    elif tag_name == 'li':
        return ''.join(html_to_markdown_docs_chainlink(child, base_url) for child in element.contents).strip()
    elif tag_name == 'a':
        href = element.get('href', '').strip()
        url = urljoin(base_url, href) if not href.startswith('http') else href
        text = children_text.replace('\n', ' ').replace('  ', ' ')
        return f"[{text}]({url}) "
    # Add additional HTML tag handling as needed

    return children_text


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

from selenium.webdriver.common.by import By


def get_all_website_links(url):
    """
    Use Selenium to navigate the website and retrieve all unique URLs.
    Clicks on the button with the class 'a.ml-auto' to navigate the site.
    """
    # Set up Chrome options
    driver = return_driver()

    urls = set()  # Set to keep track of URLs

    try:
        # Open the webpage
        driver.get(url)

        # Wait for the page to load, if necessary
        driver.implicitly_wait(5)

        # Find the button with class 'a.ml-auto' and click it
        next_page_button = driver.find_element(By.CSS_SELECTOR, 'a.ml-auto')
        next_page_button.click()

        # Wait for the page to load after click
        driver.implicitly_wait(5)

        # Add the URL of the new page to the set
        urls.add(driver.current_url)

        # Here you can add more navigation steps, find more links, etc.
        # For example, to keep on clicking on the next page and collect URLs.

    finally:
        # Close the browser session
        driver.quit()

    return urls


if __name__ == '__main__':
    # Assuming the root_directory() function is defined elsewhere and returns the correct path.
    # For demonstration, let's assume the root directory is the current working directory.
    # root_dir = root_directory()

    # Example usage:
    all_urls = get_all_website_links("https://docs.blast.io/")
    print(all_urls)

    # Paths are constructed based on the assumed root directory function.
    # papers_directory = Path(f'{root_dir}/data/papers_pdf_downloads')
    # details_csv = Path(f'{root_dir}/data/paper_details.csv')

    # Call the function and print the result.
    # Note: This will only work if the actual data exists in the specified paths.
    # unmatched_pdfs_df = get_unmatched_pdfs(papers_directory, details_csv)
    # unique_domains = extract_unique_domains_from_dataframe(unmatched_pdfs_df)
    # print(unique_domains)

    # Paths are constructed based on the assumed root directory function.
    # articles_directory = Path(f'{root_dir}/data/articles_pdf_download')
    # article_details_csv = Path(f'{root_dir}/data/links/articles_updated.csv')

    # Call the function and print the result.
    # Note: This will only work if the actual data exists in the specified paths.
    # unmatched_pdfs_articles_df = get_unmatched_pdfs(articles_directory, article_details_csv)
    # unique_domains = extract_unique_domains_from_dataframe(unmatched_pdfs_articles_df, link_field='article')
    # print(unique_domains)

    # Since we cannot execute with actual file paths in this environment, the above lines are commented out.
    # They are provided for reference to show how the function should be called in a real scenario.
