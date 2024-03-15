import base64
import logging
import os
from urllib.parse import urljoin, urlparse

import pdfkit
import requests
from bs4 import BeautifulSoup

from src.populate_csv_files.get_article_content.utils import html_to_markdown_docs_chainlink, html_to_markdown_docs, markdown_to_html
from src.utils import root_directory


def crawl_chainlink(config, overwrite):
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
                save_page_as_pdf(parsed_data, url, overwrite, base_name=config['base_name'])
            else:
                logging.error(f"Failed to fetch content for {url}")


def fetch_sidebar_urls(soup, base_url, sidebar_selector="div.lg\:text-sm"):
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

    return sidebar_links


def crawl_galadriel(config, overwrite):
    content = fetch_page(config['base_url'])
    if content:
        soup = BeautifulSoup(content, 'html.parser')

        # Get all hrefs from these links, ensuring they start with '/'
        urls_to_crawl = fetch_sidebar_urls(soup, config['base_url'])
        # if base_url in urls_to_crawl, remove it
        if config['base_url'] in urls_to_crawl:
            urls_to_crawl.remove(config['base_url'])

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
                save_page_as_pdf(parsed_data, url, overwrite, base_name=config.get('base_name', ''))
            else:
                logging.error(f"Failed to fetch content for {url}")


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
    save_path = os.path.join(save_dir(), domain + base_name, filename)

    if not overwrite and os.path.exists(save_path):
        logging.info(f"PDF already exists and overwrite is False: {save_path}")
        return

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    html_content = markdown_to_html(parsed_data['content'])
    save_as_pdf(html_content, save_path)


def save_dir():
    return f"{root_directory()}/data/ethglobal_hackathon/"


site_configs = {
    'eigenlayer': {
        'base_url': 'https://docs.eigenlayer.xyz/eigenlayer/overview',
        'content_selector': '.docItemCol_VOVn',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'galadriel': {
        'base_url': 'https://docs.galadriel.com/overview',
        'content_selector': '#content-area',
        'img_selector': "div[class*='prose'] img",
        'crawl_func': crawl_galadriel,  # Directly reference the specific crawl function
    },
    'chainlink': {
        'base_url': 'https://docs.chain.link/ccip#overview',
        'content_selector': '#article',
        'img_selector': "img",
        'html_parser': html_to_markdown_docs_chainlink,
        'crawl_func': crawl_chainlink,  # Directly reference the specific crawl function
        'base_name': '-ccip',
    },
    'chainlink_data_feeds': {
        'base_url': 'https://docs.chain.link/data-feeds',
        'content_selector': '#article',
        'img_selector': "img",
        'html_parser': html_to_markdown_docs_chainlink,
        'crawl_func': crawl_chainlink,  # Directly reference the specific crawl function
        'base_name': '-data_feeds',
    },
    'chainlink_data_streams': {
        'base_url': 'https://docs.chain.link/data-streams',
        'content_selector': '#article',
        'img_selector': "img",
        'html_parser': html_to_markdown_docs_chainlink,
        'crawl_func': crawl_chainlink,  # Directly reference the specific crawl function
        'base_name': '-data_streams',
    },
    'chainlink_functions': {
        'base_url': 'https://docs.chain.link/chainlink-functions',
        'content_selector': '#article',
        'img_selector': "img",
        'html_parser': html_to_markdown_docs_chainlink,
        'crawl_func': crawl_chainlink,  # Directly reference the specific crawl function
        'base_name': '-functions',
    },
    'chainlink_automation': {
        'base_url': 'https://docs.chain.link/chainlink-automation',
        'content_selector': '#article',
        'img_selector': "img",
        'html_parser': html_to_markdown_docs_chainlink,
        'crawl_func': crawl_chainlink,  # Directly reference the specific crawl function
        'base_name': '-automation',
    },
    'chainlink_vrf': {
        'base_url': 'https://docs.chain.link/vrf',
        'content_selector': '#article',
        'img_selector': "img",
        'html_parser': html_to_markdown_docs_chainlink,
        'crawl_func': crawl_chainlink,  # Directly reference the specific crawl function
        'base_name': '-vrf',
    },
}