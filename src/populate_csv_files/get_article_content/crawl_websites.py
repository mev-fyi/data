import csv
import logging
import time
import re
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup

from data.links.website_to_crawl_configs import sites_config
from src.utils import root_directory, return_driver

logging.basicConfig(level=logging.INFO, filename='scraping_log.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

global_exclude_links = ["signin?", "/followers?", "/about?"]
exclude_pattern = re.compile(
    r'https://medium\.com/(swlh|hackernoon|coinmonks)[^ ]*?\?source=user_profile|'
    r'/tag/[^ ]*?\?source=user_profile|'
    r'/@[^/]+/tag/[^/]+[^ ]*?-+|'
    r'/@([^/]+)/@\1[^ ]*?-+'  # Exclude repeated author URLs
)




def parse_site(site_name, site):
    unique_urls = set()
    output_rows = []

    logging.info(f"Fetching {site_name} content from {site['table_page_url']}")

    if site.get("use_selenium", False):
        driver = return_driver()
        driver.get(site["table_page_url"])
        time.sleep(3)  # Increased time for dynamic content loading
        content = driver.page_source
        driver.quit()  # Ensures the driver closes properly
    else:
        response = requests.get(site["table_page_url"])
        if response.status_code != 200:
            logging.error(f"Failed to fetch {site_name}: HTTP {response.status_code}")
            return
        content = response.text

    soup = BeautifulSoup(content, 'html.parser')
    container = soup.select_one(site["table_wrapper_selector"])

    if container:
        row_selectors = site.get("row_selectors", ["a"])  # Defaults to ["a"] if row_selectors is not specified
        for selector in row_selectors:
            links = container.select(selector)
            for link in links:
                href = link.get('href', '')
                if href:
                    # Strip unwanted parts from the URL
                    href = re.sub(r'\?source=user_profile[-_0-9]*', '', href)
                    href = re.sub(r'\?source=collection_home[-_0-9]*', '', href)
                    href = re.sub(r'/@[^/]+/tag/[^/]+[^ ]*?-+', '', href)  # Remove repeated tag parts
                    href = re.sub(r'^https://medium.com/@[^/]+/?$', '', href)  # Remove URLs with only author name
                    href = re.sub(r'^(https://medium.com/@[^/]+)/\1$', r'\1', href)  # Remove duplicated author names

                    # Check if the href is already a full URL
                    if href.startswith('http'):
                        full_url = href
                    else:
                        # Remove any redundant parts of the URL path (like '@username/')
                        path = re.sub(r'(@[^/]+)/.*', r'\1', href)
                        full_url = f"{site['base_url'].rstrip('/')}/{path.lstrip('/')}"

                    # Check for global excludes and custom exclude patterns
                    if not any(exclude in full_url for exclude in global_exclude_links) and not exclude_pattern.search(full_url):
                        if full_url not in unique_urls:
                            unique_urls.add(full_url)
                            output_rows.append([site['base_url'], full_url])
                            logging.info(f"Scraped URL: {full_url}")

    else:
        logging.warning(f"No content block found for {site_name} using selector {site['table_wrapper_selector']}")

    return output_rows


def parse_links_from_config_parallel(output_csv_path, workers=5):
    with open(output_csv_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Base URL', 'article'])

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(parse_site, site_name, site) for site_name, site in sites_config.items()]
            for future in futures:
                result = future.result()
                if result:
                    for row in result:
                        csvwriter.writerow(row)


if __name__ == '__main__':
    output_csv_path = f"{root_directory()}/data/crawled_articles.csv"

    site_names =None#['MediumPublication_wintermute']
    if site_names is not None:
        sites_config = {site_name: sites_config[site_name] for site_name in site_names if site_name in sites_config}
    parse_links_from_config_parallel(output_csv_path, workers=20)  # Example with 4 workers
