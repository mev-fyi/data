import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException

from src.populate_csv_files.get_docs.docs import start_urls
from src.utils import return_driver, root_directory

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_all_website_links_selenium(url, driver):
    try:
        driver.set_page_load_timeout(30)  # Set a page load timeout
        driver.get(url)
    except (TimeoutException, WebDriverException) as e:
        logging.error(f"Error loading {url}: {e}")
        return set()  # Return an empty set on error

    if "HTTP Error 429" in driver.page_source:
        logging.warning(f"HTTP Error 429: Too Many Requests for {url}")
        return set()

    soup = BeautifulSoup(driver.page_source, "html.parser")
    urls = set()
    domain_name = urlparse(url).netloc
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            continue
        full_url = urljoin(url, href.split('#')[0])  # Strip fragment.
        if not is_valid_url(full_url):
            continue
        if domain_name in full_url:
            urls.add(full_url)
    return urls

def save_urls(urls, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:  # Open in append mode
        fieldnames = ['URL']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if os.stat(filename).st_size == 0:  # Write header only if file is empty
            writer.writeheader()
        for url in urls:
            writer.writerow({'URL': url})

def crawl_website_selenium(start_url, intermediate_save_interval=20):
    driver = return_driver()
    visited_urls = set()
    urls_to_visit = get_all_website_links_selenium(start_url, driver)
    count = 0

    data_dir = os.path.join(root_directory(), "data", "docs")
    base_name = urlparse(start_url).netloc.replace('.', '_') + urlparse(start_url).path.replace('/', '_').rstrip('_')
    filename = os.path.join(data_dir, f"{base_name}.csv")

    try:
        while urls_to_visit:
            current_url = urls_to_visit.pop()
            if current_url in visited_urls:
                continue
            logging.info(f"Visiting: {current_url}")
            visited_urls.add(current_url)
            new_urls = get_all_website_links_selenium(current_url, driver)
            urls_to_visit = urls_to_visit.union(new_urls)

            # Intermediate save
            if count % intermediate_save_interval == 0 and count > 0:
                save_urls(visited_urls, filename)
                visited_urls.clear()  # Clear visited after saving to manage memory
            count += 1

    except WebDriverException as e:
        logging.error(f"WebDriver exception for {start_url}: {e}")
    finally:
        save_urls(visited_urls, filename)  # Final save
        driver.quit()  # Ensure the driver quits even if an error occurs

def crawl_websites_in_parallel(start_urls):
    with ThreadPoolExecutor(max_workers=min(len(start_urls), 15)) as executor:
        future_to_url = {executor.submit(crawl_website_selenium, url): url for url in start_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
                logging.info(f"Completed crawling {url}")
            except Exception as exc:
                logging.error(f"{url} generated an exception: {exc}")

if __name__ == "__main__":
    crawl_websites_in_parallel(start_urls)
