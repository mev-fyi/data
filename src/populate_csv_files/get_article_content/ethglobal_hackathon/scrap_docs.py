import requests
from bs4 import BeautifulSoup, NavigableString
import os
import pdfkit
from urllib.parse import urlparse, urljoin
import logging

# Assuming these are implemented elsewhere correctly
from src.populate_csv_files.get_article_content.utils import markdown_to_html, html_to_markdown_docs
from src.utils import root_directory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def eigenlayer_parser(soup, url):
    content_selector = '.docItemCol_VOVn'  # Adjust as needed
    markdown_content = parse_content(soup, content_selector, url)
    return {'url': url, 'content': markdown_content, 'author': "", 'date': ""}


parsers = {
    'https://docs.eigenlayer.xyz/eigenlayer/overview': eigenlayer_parser,
    # Add more sites and their parsers here as needed
}


def save_dir():
    return f"{root_directory()}/data/ethglobal_hackathon/"

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


def crawl_site(base_url):
    parser = parsers.get(base_url)
    if not parser:
        logging.error(f"No parser found for {base_url}")
        return

    current_url = base_url
    while current_url:
        content = fetch_page(current_url)
        if content:
            soup = BeautifulSoup(content, 'html.parser')
            parsed_data = parser(soup, current_url)
            save_page_as_pdf(parsed_data, current_url)

            next_button = soup.select_one('.pagination-nav__link--next')
            if next_button and next_button.has_attr('href'):
                next_url = urljoin(current_url, next_button['href'])
                if next_url != current_url:
                    current_url = next_url
                    continue
        break


def save_page_as_pdf(parsed_data, url):
    domain = urlparse(url).netloc
    path = urlparse(url).path.strip('/')
    filename = '-'.join(filter(None, path.split('/'))) + '.pdf'

    # Update the save path to include the domain
    save_path = os.path.join(save_dir(), domain, filename)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    html_content = markdown_to_html(parsed_data['content'])
    save_as_pdf(html_content, save_path)


def update_img_src_with_absolute_urls(soup, base_url):
    for img in soup.find_all('img'):
        if img.has_attr('src'):
            img['src'] = urljoin(base_url, img['src'])


def parse_content(soup, content_selector, base_url):
    content_div = soup.select_one(content_selector)
    if content_div:
        update_img_src_with_absolute_urls(content_div, base_url)  # Ensure images have absolute URLs
        full_markdown_content = '\n'.join(html_to_markdown_docs(elem, base_url) for elem in content_div.find_all(True, recursive=False))

        # Find the first occurrence of "#" which denotes the start of the main content
        main_content_start = full_markdown_content.find('#')
        if main_content_start != -1:
            # Return everything from the first "#" to the end
            return full_markdown_content[main_content_start:]
        else:
            # If there's no "#" found, return the full content as it might be formatted differently
            return full_markdown_content
    return ""


def main():
    for base_url in parsers.keys():
        logging.info(f"Starting crawl for: {base_url}")
        crawl_site(base_url)


if __name__ == '__main__':
    main()