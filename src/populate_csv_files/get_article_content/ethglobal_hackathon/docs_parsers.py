import base64
import logging
import os
import re
import time
from urllib.parse import urljoin, urlparse
import csv

import pdfkit
import requests
from bs4 import BeautifulSoup

from src.populate_csv_files.get_article_content.utils import html_to_markdown_docs_chainlink, html_to_markdown_docs, markdown_to_html
from src.utils import root_directory, return_driver

import pandas as pd
from pathlib import Path


def update_or_append_csv(pdf_path, current_url, parsed_content, csv_path, overwrite_title, lock):
    """
    Updates or appends a row to a CSV file in a thread-safe manner using a lock.

    :param pdf_path: The file path of the PDF.
    :param current_url: The URL from which the PDF was generated.
    :param parsed_content: The parsed HTML content.
    :param csv_path: The file path of the CSV to update.
    :param overwrite_title: Boolean indicating whether to overwrite the title.
    :param lock: A threading.Lock object used to synchronize access to the CSV file.
    """
    document_name = os.path.basename(pdf_path)
    if parsed_content == '':
        logging.info(f"Skipping empty content for document [{document_name}] for [{current_url}]")
        return
    title = extract_first_header(parsed_content)
    if title is None:
        title = document_name

    row_data = pd.DataFrame([{
        'title': title,
        'authors': '',
        'pdf_link': current_url,
        'release_date': '',
        'document_name': document_name
    }])

    with lock:
        if Path(csv_path).is_file():
            df = pd.read_csv(csv_path)
            # Check if title needs to be overwritten or if a new row should be added
            if overwrite_title and any(df['pdf_link'] == current_url):
                # If overwriting, find the index of the existing row and update it
                idx = df.index[df['pdf_link'] == current_url].tolist()
                if idx:
                    df.loc[idx[0]] = row_data.iloc[0]
                    logging.info(f"Overwriting existing row for [{title}]")
            else:
                # Append the new row
                logging.info(f"Appending new row for [{title}]")
                df = pd.concat([df, row_data], ignore_index=True, sort=False)
        else:
            df = row_data

        df.to_csv(csv_path, index=False)


def fetch_page_with_selenium(url, headless=False):
    driver = return_driver(headless)  # Initialize the Selenium WebDriver
    driver.get(url)

    # Optional: wait for JavaScript to load. Adjust the sleep time as necessary.
    time.sleep(3)

    content = driver.page_source
    driver.quit()
    return content


def fetch_page_with_selenium_robust(url, shadow_host_selector='body > rapi-doc', max_attempts=3, attempt_delay=2, headless=True):
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    import logging

    driver = return_driver(headless)  # Initialize the Selenium WebDriver
    content = ""
    attempts = 0

    while attempts < max_attempts:
        try:
            driver.get(url)
            # Use WebDriverWait to wait for the shadow host element to be present.
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, shadow_host_selector))
            )
            # Directly retrieve the inner HTML of the shadow DOM content using JavaScript.
            content = driver.execute_script("""
                let shadowHost = document.querySelector(arguments[0]);
                return shadowHost.shadowRoot.innerHTML;
            """, shadow_host_selector)
            break  # Exit loop if shadow DOM content is successfully retrieved
        except TimeoutException:
            logging.info(f"Timeout waiting for element. Attempt {attempts + 1} of {max_attempts}.")
            time.sleep(attempt_delay)  # Wait before retrying
        except (NoSuchElementException, Exception) as e:
            logging.info(f"An error occurred: {e}. Attempt {attempts + 1} of {max_attempts}.")
            time.sleep(attempt_delay)  # Wait before retrying
        finally:
            attempts += 1

    driver.quit()
    if not content:
        logging.info("Failed to load shadow DOM content after maximum attempts.")
    return content


def extract_first_header(markdown_content):
    """
    Extracts the first header from the markdown content as the title.
    Cleans up the title by removing markdown-style links, keeping only the link text.
    Handles cases where the header is split across two lines (e.g., #<emoji>\n<title>).
    """
    # Regex to match markdown links and capture the link text
    markdown_link_pattern = re.compile(r'\[(.*?)\]\(https?://[^\s]+\)')

    # Preprocess markdown_content to merge split headers
    processed_content = re.sub(r'(#+\s*[^#\n]*?)\n([^#\n]+)', r'\1 \2', markdown_content)

    for line in processed_content.splitlines():
        if line.startswith('# '):
            # Remove Markdown '#' formatting for headers
            title_line = line.strip('# ').strip()

            # Remove markdown links, keeping only the link text
            cleaned_title = markdown_link_pattern.sub(r'\1', title_line)

            # Further cleanup for any specific cases, like '⭐'
            if cleaned_title == '⭐':
                return None

            return cleaned_title
    return None



def crawl_chainlink(config, lock, overwrite, headless=False):
    content = fetch_page(config['base_url'])
    if content:
        soup = BeautifulSoup(content, 'html.parser')

        # Find all the URLs in the sidebar
        sidebar_links = soup.select(".nav-groups details ul li a")

        # Get all hrefs from these links, ensuring they start with '/'
        urls_to_crawl = [urljoin(config['base_url'], link['href']) for link in sidebar_links if link.get('href', '').startswith('/')]

        # Crawl each URL found in the sidebar
        for url in urls_to_crawl:
            page_content = fetch_page(url)
            if page_content:
                page_soup = BeautifulSoup(page_content, 'html.parser')
                parsed_data = {
                    'url': url,
                    'content': parse_content(page_soup, config['content_selector'], url, config['img_selector'], html_to_markdown_docs_chainlink),
                    'author': "",  # Add author extraction if needed
                    'date': ""  # Add date extraction if needed
                }
                pdf_path = save_page_as_pdf(parsed_data, url, overwrite, config.get('base_name', ''))
                if pdf_path:
                    csv_path = os.path.join(root_directory(), "data", "docs_details.csv")
                    update_or_append_csv(pdf_path, url, parsed_data['content'], csv_path, overwrite, lock)
            else:
                logging.error(f"Failed to fetch content for {url}")


def fetch_sidebar_urls(soup, base_url, sidebar_selector):
    """
    Fetch all URLs from the sidebar, regardless of their nesting level.

    :param soup: BeautifulSoup object of the webpage.
    :param base_url: The base URL of the website to ensure absolute URLs.
    :return: List of URLs to crawl.
    """
    sidebar_links = []
    # Target the sidebar directly, if possible, to reduce search scope
    sidebar_container = soup.select_one(sidebar_selector)

    if sidebar_container:
        # Recursively fetch all 'a' tags regardless of their nesting level within the sidebar
        all_links = sidebar_container.find_all("a")
        for link in all_links:
            href = link.get('href', '')
            # Ensure the link is not empty and starts with '/' (or adjust according to your needs)
            if href.startswith('/'):
                full_url = urljoin(base_url, href)
                sidebar_links.append(full_url)
            else:  # add '/'
                full_url = urljoin(base_url, '/' + href)
                sidebar_links.append(full_url)


    return sidebar_links


def fetch_sidebar_urls_wihtout_href(soup, base_url, sidebar_selector):
    """
    Fetch all URLs from the sidebar, regardless of their nesting level.

    :param soup: BeautifulSoup object of the webpage.
    :param base_url: The base URL of the website to ensure absolute URLs.
    :param sidebar_selector: The CSS selector for the sidebar.
    :return: List of URLs to crawl.
    """
    sidebar_links = []
    # Target the sidebar directly, if possible, to reduce search scope
    sidebar_container = soup.select_one(sidebar_selector)

    if sidebar_container:
        # Recursively fetch all 'a' tags regardless of their nesting level within the sidebar
        all_links = sidebar_container.find_all("a")
        for link in all_links:
            href = link.get('href', '')
            # Ensure the link is not empty
            if href and not href.startswith('#'):
                full_url = urljoin(base_url, href)
                sidebar_links.append(full_url)

    return sidebar_links


def crawl_sidebar(config, overwrite, sidebar_selector, lock, visited_urls, selenium=False, robust=False, headless=False):
    try:
        content = fetch_page_with_selenium(config['base_url']) if not robust else fetch_page_with_selenium_robust(config['base_url'])
        if content:
            soup = BeautifulSoup(content, 'html.parser')
            fetch_sidebar_func = config.get('fetch_sidebar_func', fetch_sidebar_urls)
            urls_to_crawl = fetch_sidebar_func(soup, config['base_url'], sidebar_selector)

            for url in urls_to_crawl:
                if url in visited_urls:
                    continue  # Skip this URL if it has already been visited
                visited_urls.add(url)

                if selenium:
                    page_content = fetch_page_with_selenium(url, headless)
                else:
                    page_content = fetch_page(url)
                if page_content:
                    page_soup = BeautifulSoup(page_content, 'html.parser')
                    parsed_data = {
                        'url': url,
                        'content': parse_content(page_soup, config['content_selector'], url, config['img_selector'], config.get('html_parser', html_to_markdown_docs)),
                        'author': "",  # Optionally extract author information
                        'date': ""  # Optionally extract date information
                    }
                    pdf_path = save_page_as_pdf(parsed_data, url, overwrite, config.get('base_name', ''))
                    if pdf_path:
                        csv_path = os.path.join(root_directory(), "data", "docs_details.csv")
                        update_or_append_csv(pdf_path, url, parsed_data['content'], csv_path, overwrite, lock)
                else:
                    logging.error(f"Failed to fetch content for {url}")
    except Exception as e:
        logging.error(f"Error while crawling {config['base_url']}: {e}")



def embed_images_as_data_urls(soup, base_url, img_selector):
    for img in soup.select(img_selector):
        src = img.get('src')
        if src:
            img_url = urljoin(base_url, src)
            try:
                response = requests.get(img_url, timeout=10)
                if response.status_code == 200:
                    img_type = response.headers['Content-Type'].split('/')[-1]  # More accurate MIME type
                    img_data = base64.b64encode(response.content).decode('utf-8')
                    img['src'] = f"data:image/{img_type};base64,{img_data}"
            except Exception as e:
                logging.error(f"Failed to process image {img_url}: {e}")


def fetch_page(url):
    try:
        response = requests.get(url, timeout=10)  # Added timeout for robustness
        return response.text if response.status_code == 200 else None
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None


def save_as_pdf(html_content, output_path):
    options = {
        'enable-local-file-access': True,
        'load-error-handling': 'ignore',
        'load-media-error-handling': 'ignore'
    }
    try:
        pdfkit.from_string(html_content, output_path, options=options)
        logging.info(f"Saved PDF: {output_path}")
    except Exception as e:
        logging.error(f"Error saving PDF {output_path}: {e}")


def parse_content(soup, content_selector, base_url, img_selector, html_parser=html_to_markdown_docs):
    content_div = soup.select_one(content_selector)
    if content_div:
        # embed_images_as_data_urls(content_div, base_url, img_selector)

        full_markdown_content = '\n'.join(html_parser(elem, base_url) for elem in content_div.find_all(True, recursive=False))

        # Find the first occurrence of "#" which denotes the start of the main content
        main_content_start = full_markdown_content.find('#')
        if main_content_start != -1:
            # Return everything from the first "#" to the end
            return full_markdown_content[main_content_start:]
        else:
            # If there's no "#" found, return the full content as it might be formatted differently
            return full_markdown_content
    return ""


def save_page_as_pdf(parsed_data, url, overwrite, base_name=""):
    domain = urlparse(url).netloc
    path = urlparse(url).path.strip('/')
    filename = '-'.join(filter(None, path.split('/'))) + '.pdf'
    if filename == '.pdf':
        filename = 'index.pdf'
    save_path = os.path.join(save_dir(), domain + base_name, filename)

    if not overwrite and os.path.exists(save_path):
        logging.info(f"PDF already exists and overwrite is False: {save_path}")
        return

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    html_content = markdown_to_html(parsed_data['content'])
    save_as_pdf(html_content, save_path)
    return save_path


def append_to_csv(csv_path, row_data, overwrite=False):
    """
    Appends a row to the CSV file, ensuring no duplicates.
    """
    try:
        with open(csv_path, 'r+', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_rows = [row for row in reader]

            # Check if the row_data matches any existing row to avoid duplicates
            if not overwrite and any(row['pdf_link'] == row_data['pdf_link'] for row in existing_rows):
                logging.info(f"Skipping duplicate entry for {row_data['pdf_link']}")
                return

            writer = csv.DictWriter(csvfile, fieldnames=reader.fieldnames)
            writer.writerow(row_data)
    except FileNotFoundError:
        # File doesn't exist, create it and write the header and first row
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['title', 'authors', 'pdf_link', 'release_date', 'document_name']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(row_data)


def save_dir():
    return f"{root_directory()}/data/ethglobal_hackathon/"


def clean_csv_titles(csv_path=f'{root_directory()}/data/docs_details.csv'):
    import pandas as pd
    import re

    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_path)

    # Define a pattern to match "empty links" in titles
    # This pattern matches [text](link) and aims to remove it
    pattern = re.compile(r'\[.*?\]\(https?://[^\s]+\)')

    # Clean the title column by removing "empty links"
    # Ensure all inputs to the lambda are strings
    df['title'] = df['title'].apply(lambda x: pattern.sub('', str(x)).strip() if pd.notnull(x) else x)

    # Save the cleaned DataFrame back to the CSV file
    df.to_csv(csv_path, index=False)
