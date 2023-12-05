import pandas as pd
import requests
import os
import pdfkit
import logging
from concurrent.futures import ThreadPoolExecutor
import yaml
import markdown

from utils import markdown_to_html, sanitize_filename

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_file_list(GITHUB_API_URL="https://api.github.com/repos/flashbots/flashbots-writings-website/contents/content"):
    response = requests.get(GITHUB_API_URL)
    if response.status_code == 200:
        return response.json()  # Returns a list of file information
    else:
        logging.info(f"Failed to fetch file list from {GITHUB_API_URL}, response code {response.status_code}")
        return []


def fetch_content(row, output_dir, file_list):
    article_url = getattr(row, 'article')
    article_title = getattr(row, 'title', os.path.splitext(article_url.split('/')[-1])[0])

    # Process only the matching file from the list
    for file_info in file_list:
        if file_info['name'].endswith('.mdx') and file_info['name'] == article_title + '.mdx':
            mdx_url = file_info['download_url']
            mdx_content = requests.get(mdx_url).text

            # Convert to PDF
            pdf_filename = os.path.join(output_dir, sanitize_filename(article_title) + '.pdf')
            options = {
                'encoding': "UTF-8",
                'custom-header': [('Content-Encoding', 'utf-8')],
                'no-outline': None
            }
            pdfkit.from_string(markdown_to_html(mdx_content), pdf_filename, options=options)
            logging.info(f"Saved PDF for {article_url}")
            return mdx_content
    return None


def fetch_flashbots_writing_contents_and_save_as_pdf(csv_filepath, output_dir, num_articles=None, overwrite=True):
    df = pd.read_csv(csv_filepath)
    if num_articles is not None:
        df = df.head(num_articles)
    os.makedirs(output_dir, exist_ok=True)

    flashbots_writings_file_list = get_file_list()

    with ThreadPoolExecutor() as executor:
        list(executor.map(lambda row: fetch_content(row, output_dir, flashbots_writings_file_list), df.itertuples()))


# Example usage
output_directory = '/path/to/output/pdfs'
csv_file_path = '/path/to/csv/file.csv'
fetch_flashbots_writing_contents_and_save_as_pdf(csv_file_path, output_directory, num_articles=10)