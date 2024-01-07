import requests
import os
import pdfkit
import logging
from markdown import markdown
from src.utils import root_directory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_content_list(github_api_url):
    try:
        response = requests.get(github_api_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching content from {github_api_url}: {e}")
        return []

def process_directory(output_dir, api_url, overwrite=True):
    contents = get_content_list(api_url)
    for content in contents:
        if content['type'] == 'file' and content['name'].endswith(('.md', '.mdx')):
            process_file(output_dir, content, overwrite)
        elif content['type'] == 'dir':
            new_output_dir = os.path.join(output_dir, content['name'])
            new_api_url = content['url']
            os.makedirs(new_output_dir, exist_ok=True)
            process_directory(new_output_dir, new_api_url, overwrite)

def process_file(output_dir, file_info, overwrite):
    file_name = file_info['name']
    output_pdf_path = os.path.join(output_dir, file_name.replace('.md', '.pdf').replace('.mdx', '.pdf'))

    # Check if the PDF file already exists and if overwrite is False
    if not overwrite and os.path.exists(output_pdf_path):
        logging.info(f"File already exists: {output_pdf_path}. Skipping download and conversion.")
        return

    try:
        file_url = file_info['download_url']
        file_content = requests.get(file_url).text
        convert_to_pdf(file_content, output_pdf_path)
        logging.info(f"Successfully processed and saved: {output_pdf_path}")
    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")

def convert_to_pdf(markdown_content, output_pdf_path):
    try:
        html_content = '<meta charset="UTF-8">' + markdown(markdown_content, extensions=['md_in_html'])
        options = {
            'encoding': "UTF-8",
            'custom-header': [('Content-Encoding', 'utf-8')],
            'no-outline': None
        }
        pdfkit.from_string(html_content, output_pdf_path, options=options)
    except Exception as e:
        logging.error(f"Error converting to PDF for {output_pdf_path}: {e}")

def fetch_and_save_as_pdf(output_dir, repo_api_url, overwrite=True):
    try:
        os.makedirs(output_dir, exist_ok=True)
        process_directory(output_dir, repo_api_url, overwrite)
    except Exception as e:
        logging.error(f"Error fetching and saving as PDF: {e}")

def process_repositories(overwrite=True):
    # Flashbots Docs
    # flashbots_output_dir = f'{root_directory()}/data/flashbots_docs_pdf/'
    # flashbots_repo_api_url = "https://api.github.com/repos/flashbots/flashbots-docs/contents/docs"
    # fetch_and_save_as_pdf(flashbots_output_dir, flashbots_repo_api_url)

    # Ethereum Org Website
    ethereum_output_dir = f'{root_directory()}/data/ethereum_org_website_content/'
    ethereum_repo_api_url = "https://api.github.com/repos/ethereum/ethereum-org-website/contents/src/content"
    fetch_and_save_as_pdf(ethereum_output_dir, ethereum_repo_api_url, overwrite)

if __name__ == "__main__":
    try:
        process_repositories(overwrite=False) # Change to False to avoid overwriting
    except Exception as e:
        logging.error(f"Error processing repositories: {e}")
