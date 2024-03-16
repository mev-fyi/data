import base64
import logging
import os
import re
import time
from functools import partial
from urllib.parse import urljoin, urlparse
import csv

import pdfkit
import requests
from bs4 import BeautifulSoup

from src.populate_csv_files.get_article_content.utils import html_to_markdown_docs_chainlink, html_to_markdown_docs, markdown_to_html
from src.utils import root_directory, return_driver


def fetch_page_with_selenium(url):
    driver = return_driver()  # Initialize the Selenium WebDriver
    driver.get(url)

    # Optional: wait for JavaScript to load. Adjust the sleep time as necessary.
    time.sleep(1)

    content = driver.page_source
    driver.quit()
    return content

def extract_first_header(markdown_content):
    """
    Extracts the first header from the markdown content as the title.
    If the first header contains a link, tries to extract only the text part before the link.
    """
    for line in markdown_content.splitlines():
        if line.startswith('# '):
            # Remove Markdown formatting for headers
            title_line = line.strip('# ').strip()
            # Check if title contains "[]"
            if "[]" in title_line:
                # Split on "[]" and take everything before it
                title_before_link = title_line.split("[]")[0].strip()
                if title_before_link == '⭐':
                    return None
                return title_before_link
            else:
                # If no "[]" is found, return the entire title line
                return None
    return None




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
                pdf_path = save_page_as_pdf(parsed_data, url, overwrite, config.get('base_name', ''))
                if pdf_path:
                    title = extract_first_header(parsed_data['content'])
                    document_name = os.path.basename(pdf_path)
                    row_data = {
                        'title': title if title is not None else document_name,
                        'authors': '',  # Update this if you have author information
                        'pdf_link': url,
                        'release_date': '',
                        'document_name': document_name
                    }
                    csv_path = os.path.join(root_directory(), "data", "docs_details.csv")
                    append_to_csv(csv_path, row_data)
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

    return sidebar_links


def crawl_sidebar(config, overwrite, sidebar_selector, selenium=False):
    # Use Selenium to fetch the initial page content
    content = fetch_page_with_selenium(config['base_url'])
    if content:
        soup = BeautifulSoup(content, 'html.parser')

        # Your existing logic to process the sidebar and subsequent pages
        urls_to_crawl = fetch_sidebar_urls(soup, config['base_url'], sidebar_selector)
        if config['base_url'] in urls_to_crawl:
            urls_to_crawl.remove(config['base_url'])

        for url in urls_to_crawl:
            # Use Selenium again for each page in the sidebar
            if selenium:
                page_content = fetch_page_with_selenium(url)
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
                    # Your existing logic to extract header and append to CSV
                    title = extract_first_header(parsed_data['content'])
                    document_name = os.path.basename(pdf_path)
                    row_data = {
                        'title': title if title is not None else document_name,
                        'authors': '',
                        'pdf_link': url,
                        'release_date': '',
                        'document_name': document_name
                    }
                    csv_path = os.path.join(root_directory(), "data", "docs_details.csv")
                    append_to_csv(csv_path, row_data)
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


def append_to_csv(csv_path, row_data):
    """
    Appends a row to the CSV file, ensuring no duplicates.
    """
    try:
        with open(csv_path, 'r+', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_rows = [row for row in reader]

            # Check if the row_data matches any existing row to avoid duplicates
            if any(row['pdf_link'] == row_data['pdf_link'] for row in existing_rows):
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
        'crawl_func': partial(crawl_sidebar, sidebar_selector='div.lg\:text-sm'),  # Directly reference the specific crawl function
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
    'nethermind': {
        'base_url': 'https://docs.nethermind.io',
        'content_selector': '.docItemCol_VOVn',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'blockscout': {
        'base_url': 'https://docs.blockscout.com',
        'content_selector': 'main.flex-1',
        'img_selector': 'picture.relative',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.pt-4 > ul:nth-child(1)'),  # Directly reference the specific crawl function
    },
    'pimlico_permissionless': {
        'base_url': 'https://docs.pimlico.io/permissionless',
        'content_selector': '.vocs_Content',
        'img_selector': 'picture.relative',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.vocs_Sidebar_group'),
        'base_name': '-permissionless',
    },
    'pimlico_bundler': {
        'base_url': 'https://docs.pimlico.io/bundler',
        'content_selector': '.vocs_Content',
        'img_selector': 'picture.relative',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.vocs_Sidebar_group'),
        'base_name': '-bundler',
    },
    'pimlico_paymaster': {
        'base_url': 'https://docs.pimlico.io/paymaster',
        'content_selector': '.vocs_Content',
        'img_selector': 'picture.relative',
        'base_name': '-paymaster',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.vocs_Sidebar_group'),

    },
    'argent': {
        'base_url': 'https://docs.argent.xyz/',
        'content_selector': 'main.flex-1',
        'img_selector': 'picture.relative',
        'base_name': '',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='aside.relative'),
    },
    'Obol': {
        'base_url': 'https://docs.obol.tech/docs/int/Overview',
        'content_selector': '.docItemCol_VOVn',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'lido': {
        'base_url': 'https://docs.lido.fi/',
        'content_selector': '.docItemCol_VOVn',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'giza_datasets': {
        'base_url': 'https://datasets.gizatech.xyz/',
        'content_selector': 'main.flex-1',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.pt-4 > ul:nth-child(1)'),
    },
    'giza_orion': {
        'base_url': 'https://orion.gizatech.xyz/',
        'content_selector': 'main.flex-1',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.pt-4 > ul:nth-child(1)'),
    },
    'giza_cli': {
        'base_url': 'https://cli.gizatech.xyz/',
        'content_selector': 'main.flex-1',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.pt-4 > ul:nth-child(1)'),
    },
    'giza_actions': {
        'base_url': 'https://actions.gizatech.xyz/',
        'content_selector': 'main.flex-1',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.pt-4 > ul:nth-child(1)'),
    },
    'cairo': {
        'base_url': 'https://docs.cairo-lang.org/',
        'content_selector': '.doc',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.next > a:nth-child(1)',
    },
    'circle_stablecoins': {
        'base_url': 'https://developers.circle.com/stablecoins/',
        'content_selector': '.rm-Article',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.Sidebar1t2G1ZJq-vU1'),
    },
    'circle_w3s': {
            'base_url': 'https://developers.circle.com/w3s/docs/circle-programmable-wallets-an-overview',
            'content_selector': '.rm-Article',
            'img_selector': '.img_ev3q',
            'crawl_func': partial(crawl_sidebar, sidebar_selector='.Sidebar1t2G1ZJq-vU1'),
    },
    'circle_mint': {
            'base_url': 'https://developers.circle.com/circle-mint/docs/introducing-circle-mint',
            'content_selector': '.rm-Article',
            'img_selector': '.img_ev3q',
            'crawl_func': partial(crawl_sidebar, sidebar_selector='.Sidebar1t2G1ZJq-vU1'),
    },
    'circle_verite': {
        'base_url': 'https://developers.circle.com/verite/docs/verite-protocol-introduction',
        'content_selector': '.rm-Article',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.Sidebar1t2G1ZJq-vU1'),
    },
    'hyperlane': {
        'base_url': 'https://docs.hyperlane.xyz/docs/intro',
        'content_selector': '.docItemCol_VOVn',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'safe_home': {
        'base_url': 'https://docs.safe.global/home/what-is-safe',
        'content_selector': 'main.nx-w-full',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.nextra-menu-desktop'),
    },
    'safe_sdk': {
        'base_url': 'https://docs.safe.global/sdk/overview',
        'content_selector': 'main.nx-w-full',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.nextra-menu-desktop'),
    },
    'safe_advanced': {
        'base_url': 'https://docs.safe.global/advanced/api-service-architecture',
        'content_selector': 'main.nx-w-full',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.nextra-menu-desktop'),
    },
    'farcaster': {
        'base_url': 'https://docs.farcaster.xyz/',
        'content_selector': '.main',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='#VPSidebarNav'),
    },
    'near_concepts': {
        'base_url': 'https://docs.near.org/concepts/welcome',
        'content_selector': '.docItemContainer_Djhp',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'near_web3_apps': {
        'base_url': 'https://docs.near.org/develop/web3-apps/whatareweb3apps',
        'content_selector': '.docItemContainer_Djhp',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'near_tools': {
        'base_url': 'https://docs.near.org/tools/welcome',
        'content_selector': '.docItemContainer_Djhp',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'near_contracts': {
        'base_url': 'https://docs.near.org/develop/contracts/whatisacontract',
        'content_selector': '.docItemContainer_Djhp',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'near_tutorials': {
        'base_url': 'https://docs.near.org/tutorials/welcome',
        'content_selector': '.docItemContainer_Djhp',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'celo': {
        'base_url': 'https://docs.celo.org/developer',
        'content_selector': '.docItemCol_VOVn',
        'img_selector': '.img_ev3q',
        'next_button_selector': '.pagination-nav__link--next',
    },
    'base': {
        'base_url': 'https://docs.base.org/',
        'content_selector': '.docItemContainer_Rv5Z',
        'img_selector': '.img_ev3q',
        'crawl_func': partial(crawl_sidebar, sidebar_selector='.theme-doc-sidebar-menu', selenium=True),
        # 'next_button_selector': '.pagination-nav__link--next',
    },
}
