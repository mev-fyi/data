import pandas as pd
import requests
import os
import pdfkit
import logging
from concurrent.futures import ThreadPoolExecutor
import yaml
import markdown

from src.utils import root_directory
from utils import markdown_to_html, sanitize_filename

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_file_list(GITHUB_API_URL="https://api.github.com/repos/flashbots/flashbots-writings-website/contents/content"):
    response = requests.get(GITHUB_API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        logging.info(f"Failed to fetch file list from {GITHUB_API_URL}, response code {response.status_code}")
        return []


def extract_title_from_mdx(mdx_content, default_title):
    try:
        lines = mdx_content.split('\n')
        if lines[0] == '---':
            end_of_front_matter = lines.index('---', 1)
            front_matter = '\n'.join(lines[1:end_of_front_matter])
            front_matter_yaml = yaml.safe_load(front_matter)
            return front_matter_yaml.get('title', default_title)
    except Exception as e:
        logging.error(f"Error extracting title: {e}")
    return default_title


def convert_mdx_to_pdf(mdx_content, output_pdf_path):
    html_content = '<meta charset="UTF-8">' + markdown.markdown(mdx_content)
    options = {
        'encoding': "UTF-8",
        'custom-header': [('Content-Encoding', 'utf-8')],
        'no-outline': None
    }
    pdfkit.from_string(html_content, output_pdf_path, options=options)


def process_file_info(output_dir, file_info):
    if file_info['name'].endswith('.mdx'):
        mdx_url = file_info['download_url']
        mdx_content = requests.get(mdx_url).text
        default_title = os.path.splitext(file_info['name'])[0]
        title = extract_title_from_mdx(mdx_content, default_title)
        file_name = title + '.pdf'
        output_pdf_path = os.path.join(output_dir, file_name)
        convert_mdx_to_pdf(mdx_content, output_pdf_path)
        logging.info(f"Processed {file_name}")


def fetch_flashbots_writing_contents_and_save_as_pdf(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    flashbots_writings_file_list = get_file_list()
    with ThreadPoolExecutor() as executor:
        executor.map(lambda file_info: process_file_info(output_dir, file_info), flashbots_writings_file_list)


output_directory = f'{root_directory()}/data/articles_pdf_download/'
fetch_flashbots_writing_contents_and_save_as_pdf(output_directory)
