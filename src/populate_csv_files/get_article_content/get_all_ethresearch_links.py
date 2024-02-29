import logging
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from src.utils import return_driver, root_directory

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def scrape_ethresearch_links():
    # Initialize WebDriver
    driver = return_driver()
    driver.get("https://ethresear.ch/")

    def scroll_to_bottom():
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Wait to load page
            time.sleep(2)
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            logging.info("Scrolled to current bottom of the page.")

    # Scroll to the very first post
    scroll_to_bottom()

    # Extract links
    links = []
    topics = driver.find_elements(By.CSS_SELECTOR, ".topic-list-item .main-link")
    for topic in topics:
        link = topic.get_attribute('href')
        links.append(link)

    # Save links to CSV
    with open(f'{root_directory()}/data/links/ethresearch_links.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Link'])  # Header
        for link in links:
            writer.writerow([link])

    logging.info(f"Total {len(links)} links have been extracted and saved to CSV.")

    # Close the driver
    driver.quit()


if __name__ == "__main__":
    logging.info("Starting to scrape ethresear.ch for links...")
    scrape_ethresearch_links()
    logging.info("Scraping completed.")
