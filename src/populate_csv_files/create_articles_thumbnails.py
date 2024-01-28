import functools
import os
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


def sanitize_title(title):
    # Replace '/' with '<slash>'
    title_with_slash_replaced = title.replace('/', '<slash>')

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


def take_screenshot(url, title, output_dir, headless=False, zoom=145, screenshot_height_percent=0.20, max_height=900, min_height=600):
    driver = return_driver(headless)

    attempt = 0
    page_loaded = False
    while attempt < 2 and not page_loaded:
        try:
            driver.get(url)
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

    formatted_title = sanitize_title(title)
    screenshot_path = os.path.join(output_dir, f"{formatted_title}.png")
    cropped_image.save(screenshot_path)
    driver.quit()
    logging.info(f"Saved screenshot for {url} at {screenshot_path}")


def process_row(row, headless: bool):
    output_dir = f"{root_directory()}/data/article_thumbnails"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if 'article' in row.keys():
        take_screenshot(row['article'], row['title'], output_dir, headless)
    elif 'pdf_link' in row.keys():
        take_screenshot(row['pdf_link'], row['title'], output_dir, headless)
    else:
        logging.error("Could not find link in row")


def main(headless: bool):
    logging.basicConfig(level=logging.INFO)
    csv_file_paths = [
        f'{root_directory()}/data/links/articles_updated.csv',
        f'{root_directory()}/data/docs_details.csv'
    ]
    for csv_file_path in csv_file_paths:
        try:
            df = pd.read_csv(csv_file_path)
        except FileNotFoundError:
            logging.error(f"File not found: {csv_file_path}")
            continue

        # Filter for specific domains if necessary
        # df = df[df['article'].str.contains('medium.com')]

        num_workers = 20  # None # int(os.cpu_count() // 2)
        os.environ['NUMEXPR_MAX_THREADS'] = f'{num_workers}'  # You can adjust the number as needed
        process_row_with_headless = functools.partial(process_row, headless=headless)
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(process_row_with_headless, df.to_dict('records'))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main(headless=False)
