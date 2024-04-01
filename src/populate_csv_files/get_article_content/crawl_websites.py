import csv
import logging
import time

import requests
from bs4 import BeautifulSoup
import re

from data.links.website_to_crawl_configs import sites_config
from src.utils import root_directory, return_driver

logging.basicConfig(level=logging.INFO, filename='scraping_log.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

global_exclude_links = ["signin?", "/followers?", "/about?"]
exclude_pattern = re.compile(r'https://medium\.com/(swlh|hackernoon|coinmonks|tag)[^ ]*?\?source=user_profile')


def parse_links_from_config(output_csv_path):
    unique_urls = set()

    with open(output_csv_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Base URL', 'Article URL'])

        for site_name, site in sites_config.items():
            logging.info(f"Fetching {site_name} content from {site['table_page_url']}")

            if site.get("use_selenium", False):
                driver = return_driver()
                driver.get(site["table_page_url"])
                time.sleep(2)  # Allow time for content to load
                content = driver.page_source
                driver.close()
            else:
                response = requests.get(site["table_page_url"])
                if response.status_code != 200:
                    logging.error(f"Failed to fetch {site_name}: HTTP {response.status_code}")
                    continue
                content = response.text

            soup = BeautifulSoup(content, 'html.parser')
            container = soup.select_one(site["table_wrapper_selector"])

            if container:
                process_links(container, site, unique_urls, csvwriter)
            else:
                logging.warning(f"No content block found for {site_name} using selector {site['table_wrapper_selector']}")

def process_links(container, site, unique_urls, csvwriter):
    links = container.select(site.get("row_selector_template", "a"))
    for link in links:
        href = link.get('href', '')
        # Check against global_exclude_links and the regex pattern
        if href and not any(exclude in href for exclude in global_exclude_links + site.get("exclude_links", [])) and not exclude_pattern.search(href):
            # Proceed with processing the link
            full_url = href if href.startswith('http') else f"{site['base_url'].rstrip('/')}/{href.lstrip('/')}"
            if full_url not in unique_urls:
                unique_urls.add(full_url)
                csvwriter.writerow([site['base_url'], full_url])
                logging.info(f"Scrapped URL: {full_url}")

output_csv_path = f"{root_directory()}/data/crawled_articles.csv"
parse_links_from_config(output_csv_path)