import json
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse, urljoin
from typing import Dict, Set

from src.utils import root_directory

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to read or create the configuration
def read_or_create_config(csv_path=f"{root_directory()}/data/links/websites.csv", json_path=f"{root_directory()}/data/website_crawl_config.json"):
    # Check if the configuration JSON file exists
    if os.path.exists(json_path):
        # Read the existing configuration JSON file
        with open(json_path, 'r') as file:
            config = json.load(file)
    else:
        # Create a skeleton configuration from the CSV file
        websites_df = pd.read_csv(csv_path)
        config = {}
        for website in websites_df['website']:
            if website not in config:  # Ensure not to overwrite existing config
                config[website] = {
                    'container': '',
                    'subcontainer': '',
                    'href_selector': '',
                    'next_page_selector': ''
                }
        # Save the newly created configuration to a JSON file
        with open(json_path, 'w') as file:
            json.dump(config, file, indent=4)

    return config


def fetch_article_urls(url: str, selectors: Dict[str, str]) -> Set[str]:
    """
    Fetches article URLs from a single page based on provided CSS selectors.

    :param url: URL of the page to scrape.
    :param selectors: CSS selectors for container, subcontainer, href, and pagination.
    :return: A set of article URLs found on the page.
    """
    base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch the webpage: {e}")
        return set()

    soup = BeautifulSoup(response.text, 'html.parser')
    article_urls = set()

    main_container = soup.select(selectors.get('container', ''))
    if not main_container:
        logging.warning("Main container not found. Skipping URL.")
        return article_urls

    for subcontainer in main_container[0].select(selectors.get('subcontainer', '')):
        for a_tag in subcontainer.select(selectors.get('href_selector', '')):
            if 'href' in a_tag.attrs:
                article_url = urljoin(base_url, a_tag['href'])
                article_urls.add(article_url)
                logging.info(f"Found article URL: {article_url}")

    return article_urls


def get_article_urls(config: Dict[str, Dict[str, str]]) -> Dict[str, Set[str]]:
    """
    Fetches article URLs based on provided configurations, including handling pagination.
    Skips websites with empty selector configurations.

    :param config: A dictionary with URLs as keys and their scraping configurations as values.
    :return: A dictionary mapping each URL to a set of found article URLs.
    """
    results: Dict[str, Set[str]] = {}

    for url, selectors in config.items():
        # Check for empty selectors
        if not all(selectors.values()):
            logging.warning(f"Selectors for URL {url} are not fully defined. Skipping...")
            continue

        try:
            logging.info(f"Processing URL: {url}")
            current_page_url: str = url
            # get the base url as subdomain.domain.extension
            base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            article_urls: Set[str] = set()

            while current_page_url:
                page_article_urls = fetch_article_urls(current_page_url, selectors)
                if page_article_urls:
                    article_urls.update(page_article_urls)
                    logging.info(f"Collected {len(page_article_urls)} articles from {current_page_url}")
                else:
                    logging.warning("No articles found or unable to fetch the page.")

                try:
                    response = requests.get(current_page_url)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    next_page_link = soup.select_one(selectors.get('next_page_selector', ''))
                    if next_page_link and 'href' in next_page_link.attrs:
                        next_page_url = urljoin(base_url, next_page_link['href'])
                        if next_page_url == current_page_url:
                            logging.info("Reached the last page. No more pagination.")
                            break
                        current_page_url = next_page_url
                        logging.info(f"Paginating to next page: {current_page_url}")
                    else:
                        logging.info("Pagination link not found. Assuming end of pages.")
                        break
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to navigate to the next page: {e}")
                    break

            results[url] = article_urls
        except Exception as e:
            logging.error(f"Error processing URL {url}: {e}")

    return results


def main() -> None:
    config = read_or_create_config()

    result = get_article_urls(config)
    for url, article_urls in result.items():
        print(f"Articles found for {url}:")
        for article_url in article_urls:
            print(article_url)


if __name__ == "__main__":
    main()
