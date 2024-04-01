import csv
import logging
import requests
from bs4 import BeautifulSoup
import re  # Import the regex module

from data.links.website_to_crawl_configs import sites_config
from src.utils import root_directory

# Set up logging
logging.basicConfig(level=logging.INFO, filename='scraping_log.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Global exclusion keywords
global_exclude_links = [
    "signin?",
    "/followers?",
    "/about?",
]

# Compile a regex pattern for URLs to be excluded
exclude_pattern = re.compile(r'https://medium\.com/(swlh|coinmonks|tag)[^ ]*?\?source=user_profile')

def parse_links_from_config(output_csv_path):
    unique_urls = set()  # Maintain a set of unique URLs

    with open(output_csv_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Base URL', 'Article URL'])

        for site_name, site in sites_config.items():
            base_url = site["base_url"]
            page_url = site.get("table_page_url", base_url)
            content_selector = site["table_wrapper_selector"]
            exclude_links = site.get("exclude_links", [])

            logging.info(f"Starting to fetch and parse links for {site_name}")

            try:
                response = requests.get(page_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                container = soup.select_one(content_selector)

                if container:
                    all_rows = container.find_all(True, recursive=False)
                    for row in all_rows:
                        links = row.select("a")
                        for link in links:
                            if link.has_attr('href'):
                                href = link['href']

                                # Check if href matches any global exclusion patterns or URLs
                                if any(exclude in href for exclude in global_exclude_links) or exclude_pattern.search(href):
                                    # logging.info(f"Excluded URL for {site_name}: {href}")
                                    continue

                                # Compose the full URL
                                full_url = href if href.startswith('http') else f"{base_url.rstrip('/')}/{href.lstrip('/')}"

                                # Check if the URL is already added
                                if full_url not in unique_urls:
                                    unique_urls.add(full_url)  # Mark this URL as added
                                    logging.info(f"Scrapped URL for {site_name}: {full_url}")
                                    csvwriter.writerow([base_url, full_url])
                else:
                    logging.warning(f"Content block not found for {site_name}")
            except requests.RequestException as e:
                logging.error(f"Failed to fetch data for {site_name}: {e}")

# Define the output CSV file path
output_csv_path = f"{root_directory()}/data/crawled_articles.csv"

# Execute the function with the path to the output file
parse_links_from_config(output_csv_path)
