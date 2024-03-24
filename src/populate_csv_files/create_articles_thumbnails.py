import functools
import os
import random
from pathlib import Path
from typing import Tuple

import pandas as pd
import time
from PIL import Image
from io import BytesIO
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import concurrent.futures
import logging
from src.utils import return_driver, root_directory


def extract_domain(url):
    from urllib.parse import urlparse
    parsed_url = urlparse(url)
    return parsed_url.netloc

def sanitize_title(title):
    # Replace '/' with '<slash>'
    title_with_slash_replaced = str(title).replace('/', '<slash>')

    # Keep all non-alphanumeric characters (except replacing '/' with '<slash>')
    # sanitized_title = re.sub(r'[^a-zA-Z0-9\-#]', ' ', title_with_slash_replaced)

    # Collapse multiple spaces
    # return re.sub(r'\s+', ' ', sanitized_title).strip()
    return title_with_slash_replaced.strip()


def close_popups(driver, url):
    if "medium.com" in url:
        popups = [
            '//*[@id="root"]/div/div[3]/div[2]/div[4]/div/div/div/div[1]/div[2]/div/button',
            '/html/body/div[2]/div/div/div/div[2]/div/button'
        ]

        for popup_xpath in popups:
            try:
                # Wait for the element to be clickable and then click
                WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, popup_xpath))).click()
            except TimeoutException:
                # If the element is not clickable after the wait, try JavaScript click
                try:
                    close_button = driver.find_element(By.XPATH, popup_xpath)
                    driver.execute_script("arguments[0].click();", close_button)
                except NoSuchElementException:
                    logging.warning(f"Popup not found for {url}.")
        time.sleep(2)


def take_screenshot(url, document_name, output_dir, overwrite, headless, zoom=145, screenshot_height_percent=0.20, max_height=900, min_height=600):
    # Check for the screenshot file existence considering the new output_dir structure
    formatted_name = str(sanitize_title(document_name)).replace('.pdf', '')
    screenshot_path = os.path.join(output_dir, f"{formatted_name}.png")

    if not overwrite and Path(screenshot_path).exists():
        logging.info(f"Screenshot for {document_name} already exists. Skipping.")
        return

    driver = return_driver(headless)
    attempt = 0
    page_loaded = False
    while attempt < 2 and not page_loaded:
        try:
            # do a random time sleep between 1 and 5 seconds
            time.sleep(random.randint(1, 5))
            driver.get(url)
            # Check for HTTP status code here
            status_code = driver.execute_script("return document.status;")
            if status_code == 429:
                logging.warning(f"HTTP 429 received for {url}. Retrying after delay...")
                time.sleep(5)  # Wait for 5 seconds before retrying
                attempt += 1
                continue
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            page_loaded = True
        except (TimeoutException, WebDriverException) as e:
            logging.error(f"Error occurred while loading {url}. Attempt: {attempt+1}, Error: {e}")
            attempt += 1
            time.sleep(5)
        except WebDriverException as e:
            logging.error(f"Error occurred: {e}. Attempt: {attempt+1}")
            attempt += 1
            time.sleep(5)

    if not page_loaded:
        logging.error(f"Failed to load page {url} after multiple attempts.")
        driver.quit()
        return

    # Close popups if necessary
    close_popups(driver, url)

    # Apply zoom
    # driver.execute_script(f"document.body.style.zoom='{zoom}%'")
    # Apply zoom and center content
    driver.execute_script(f"""
        document.body.style.zoom='{zoom}%';
        const calculatedWidth = document.body.scrollWidth * ({zoom} / 100);
        const offsetX = (calculatedWidth - window.innerWidth) / 2;
        document.body.style.transform = 'translateX(-' + offsetX + 'px)';
    """)

    # Get the total height of the page
    total_height = driver.execute_script("return document.body.parentNode.scrollHeight")

    # Calculate the desired height (percentage of the total height)
    desired_height = int(total_height * screenshot_height_percent)

    # Take screenshot of the required part of the page
    time.sleep(1)
    png = driver.get_screenshot_as_png()

    # Open the screenshot and crop the top portion
    image = Image.open(BytesIO(png))
    cropped_image = image.crop((0, 0, image.width, max(min(max_height, desired_height), min_height)))

    cropped_image.save(screenshot_path)
    driver.quit()
    logging.info(f"Saved screenshot for {url} at {screenshot_path}")


def process_row(row, headless: bool, link_key: str, overwrite: bool):
    output_base_dir = f"{root_directory()}/data/article_thumbnails"
    domain = extract_domain(row[link_key])
    output_dir = os.path.join(output_base_dir, domain)
    os.makedirs(output_dir, exist_ok=True)

    # Assume 'document_name' is a column in your DataFrame that uniquely identifies the document
    if 'document_name' in row:
        document_name = row['document_name']
    elif 'title' in row:
        document_name = row['title']
    elif 'Title' in row:
        document_name = row['Title']
    try:
        take_screenshot(row[link_key], document_name, output_dir, overwrite, headless)
    except Exception as e:
        logging.error(f"Error occurred while processing {row[link_key]}: {e}")


def prepare_df(csv_file_path, link_key):
    df = pd.read_csv(csv_file_path)
    if link_key not in df.columns:
        df.rename(columns={link_key: 'article', 'Title': 'title', 'Author': 'authors', 'Release Date': 'release_date'}, inplace=True)
    return df


def check_existing_screenshots(document_name: str, output_base_dir: str) -> bool:
    """
    Check if a screenshot for the given document name already exists.
    """
    formatted_name = sanitize_title(document_name).replace('.pdf', '') + '.png'
    screenshot_path = os.path.join(output_base_dir, formatted_name)
    return Path(screenshot_path).exists()


def filter_df_for_new_thumbnails(df, output_base_dir: str, link_key: str) -> pd.DataFrame:
    """
    Filter the DataFrame to include only rows for which thumbnails need to be generated.
    """
    # Apply a mask to filter out existing screenshots
    needs_screenshot = df.apply(lambda row: not check_existing_screenshots(row.get('document_name', row.get('title', row.get('Title', ''))), os.path.join(output_base_dir, extract_domain(row[link_key]))), axis=1)
    logging.info(f"Generating thumbnails for [{needs_screenshot.sum()}] new documents.")
    return df[needs_screenshot]


def main(csv_file_path_link_key_tuples: Tuple[str, str] = [(f'{root_directory()}/data/links/articles_updated.csv', 'article'), (f'{root_directory()}/data/docs_details.csv', 'pdf_link'), (f'{root_directory()}/data/links/merged_articles.csv', 'Link')],
         headless: bool=True, overwrite: bool=False, num_workers: int=18):
    output_base_dir = f"{root_directory()}/data/article_thumbnails"

    for csv_file_path, link_key in csv_file_path_link_key_tuples:
        try:
            df = prepare_df(csv_file_path, link_key)
            # Shuffle the DataFrame rows to randomize URL access order
            df = df.sample(frac=1).reset_index(drop=True)
            # Filter out documents that already have a generated thumbnail
            if not overwrite:
                df = filter_df_for_new_thumbnails(df, output_base_dir, link_key)
        except FileNotFoundError:
            logging.error(f"File not found: {csv_file_path}")
            continue

        os.environ['NUMEXPR_MAX_THREADS'] = str(num_workers)
        process_row_with_headless_and_link_key = functools.partial(process_row, headless=headless, link_key=link_key, overwrite=overwrite)
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(process_row_with_headless_and_link_key, df.to_dict('records'))


if __name__ == "__main__":
    csv_file_path_link_key_tuples = [
        (f'{root_directory()}/data/links/articles_updated.csv', 'article'),
        (f'{root_directory()}/data/docs_details.csv', 'pdf_link'),
        (f'{root_directory()}/data/links/merged_articles.csv', 'Link')
    ]
    main(csv_file_path_link_key_tuples, headless=False, overwrite=False, num_workers=18)