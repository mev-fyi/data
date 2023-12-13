import pandas as pd
import time
from PIL import Image
from io import BytesIO
import re

from src.utils import return_driver, root_directory

def sanitize_title(title):
    # Replace non-alphanumeric characters with a space and collapse multiple spaces
    sanitized_title = re.sub(r'[^a-zA-Z0-9]', ' ', title)
    return re.sub(r'\s+', ' ', sanitized_title).strip()

def take_screenshot(url, title, output_dir, zoom=100, screenshot_height_percent=0.15):
    driver = return_driver()

    try:
        driver.get(url)
        time.sleep(3)  # Allow for full page loading

        # Apply zoom
        driver.execute_script(f"document.body.style.zoom='{zoom}%'")

        # Get the total height of the page
        total_height = driver.execute_script("return document.body.parentNode.scrollHeight")

        # Calculate the desired height (percentage of the total height)
        desired_height = int(total_height * screenshot_height_percent)

        # Set a large fixed width to avoid horizontal scroll and adjust the height
        adjusted_width = 2000  # Set a large width to avoid horizontal scrollbar

        # Set window size to capture the required part of the page
        driver.set_window_size(adjusted_width, desired_height)

        # Take screenshot of the required part of the page
        png = driver.get_screenshot_as_png()

        # Open the screenshot and crop the top portion
        image = Image.open(BytesIO(png))
        cropped_image = image.crop((0, 0, image.width, min(desired_height, image.height)))

        # Format title for file name
        formatted_title = sanitize_title(title)

        # Save cropped screenshot
        cropped_image.save(f"{output_dir}/{formatted_title}.png")
    finally:
        driver.quit()

# Read URLs and titles from CSV
csv_file_path = f'{root_directory()}/data/links/articles_updated.csv'
df = pd.read_csv(csv_file_path)

# Output directory for screenshots
output_dir = f"{root_directory()}/data/article_thumbnails"

# Iterate over the rows in the dataframe
for index, row in df.iterrows():
    take_screenshot(row['article'], row['title'], output_dir)
    # if index == 1:  # Stop after taking two screenshots for testing
    #     break
