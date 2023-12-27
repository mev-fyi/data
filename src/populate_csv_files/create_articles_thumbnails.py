import os

import pandas as pd
import time
from PIL import Image
from io import BytesIO
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import concurrent.futures

from selenium.common.exceptions import NoSuchElementException
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
                    # If the popup is not found, pass
                    pass
        time.sleep(2)


def take_screenshot(url, title, output_dir, zoom=100, screenshot_height_percent=0.60, min_width=600, min_height=300):
    driver = return_driver()

    try:
        driver.get(url)
        time.sleep(5)  # Allow for full page loading

        # Close popups if necessary
        close_popups(driver, url)

        # Apply zoom
        driver.execute_script(f"document.body.style.zoom='{zoom}%'")

        # Get the total height of the page
        total_height = driver.execute_script("return document.body.parentNode.scrollHeight")

        # Calculate the desired height (percentage of the total height)
        desired_height = int(total_height * screenshot_height_percent)

        # Set a large fixed width to avoid horizontal scroll and adjust the height
        adjusted_width = 1200  # Set a large width to avoid horizontal scrollbar

        # Set window size to capture the required part of the page
        driver.set_window_size(adjusted_width, desired_height)

        # Take screenshot of the required part of the page
        png = driver.get_screenshot_as_png()

        # Open the screenshot and crop the top portion
        image = Image.open(BytesIO(png))
        cropped_image = image.crop((0, 0, image.width, min(desired_height, image.height)))

        # Sanity check for image dimensions
        if cropped_image.width >= min_width and cropped_image.height >= min_height:
            # Format title for file name
            formatted_title = sanitize_title(title)

            # Save cropped screenshot
            cropped_image.save(f"{output_dir}/{formatted_title}.png")
    finally:
        driver.quit()


def process_row(row):
    output_dir = f"{root_directory()}/data/article_thumbnails"
    take_screenshot(row['article'], row['title'], output_dir)


def main():
    """
    Created with 1280x720 16:9 resolution
    """
    # Read URLs and titles from CSV
    csv_file_path = f'{root_directory()}/data/links/articles_updated.csv'
    df = pd.read_csv(csv_file_path)

    # Filter for specific domains if necessary
    # df = df[df['article'].str.contains('medium.com')]

    # Determine number of workers based on the number of available cores
    num_workers = int(os.cpu_count() // 1.5)

    # Use ThreadPoolExecutor to parallelize the task
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        executor.map(process_row, df.to_dict('records'))


if __name__ == "__main__":
    main()
