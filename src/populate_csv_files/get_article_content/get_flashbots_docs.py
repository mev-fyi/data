import pandas as pd
import requests
import os
import pdfkit
import logging
from concurrent.futures import ThreadPoolExecutor
import yaml
import markdown

from src.utils import root_directory
from datetime import datetime
from src.populate_csv_files.get_article_content.utils import markdown_to_html, sanitize_filename

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_content_list(github_api_url):
    response = requests.get(github_api_url)
    if response.status_code == 200:
        return response.json()
    else:
        logging.info(f"Failed to fetch content from {github_api_url}, response code {response.status_code}")
        return []

def process_directory(output_dir, api_url):
    contents = get_content_list(api_url)
    for content in contents:
        if content['type'] == 'file' and content['name'].endswith(('.md', '.mdx')):
            process_file(output_dir, content)
        elif content['type'] == 'dir':
            new_output_dir = os.path.join(output_dir, content['name'])
            new_api_url = content['url']
            os.makedirs(new_output_dir, exist_ok=True)
            process_directory(new_output_dir, new_api_url)

def process_file(output_dir, file_info):
    file_url = file_info['download_url']
    file_content = requests.get(file_url).text
    file_name = os.path.splitext(file_info['name'])[0] + '.pdf'
    output_pdf_path = os.path.join(output_dir, file_name)
    convert_to_pdf(file_content, output_pdf_path)
    logging.info(f"Processed {file_name}")

def convert_to_pdf(markdown_content, output_pdf_path):
    try:
        html_content = '<meta charset="UTF-8">' + markdown.markdown(markdown_content)
        options = {
            'encoding': "UTF-8",
            'custom-header': [('Content-Encoding', 'utf-8')],
            'no-outline': None
        }
        pdfkit.from_string(html_content, output_pdf_path, options=options)
    except Exception as e:
        logging.error(f"Error converting to PDF: {e}")

def fetch_and_save_as_pdf(output_dir, repo_api_url):
    os.makedirs(output_dir, exist_ok=True)
    process_directory(output_dir, repo_api_url)

if __name__ == "__main__":
    output_directory = f'{root_directory()}/data/flashbots_docs_pdf/'
    repo_api_url = "https://api.github.com/repos/flashbots/flashbots-docs/contents/docs"
    fetch_and_save_as_pdf(output_directory, repo_api_url)
