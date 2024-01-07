import requests
import os
import pdfkit
import logging
from markdown import markdown

# Assuming 'src.utils' and 'src.populate_csv_files.get_article_content.utils'
# modules are properly defined elsewhere in your project.
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
    file_name = file_info['name']
    try:
        file_url = file_info['download_url']
        file_content = requests.get(file_url).text
        output_pdf_path = os.path.join(output_dir, file_name.replace('.md', '.pdf').replace('.mdx', '.pdf'))
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

def fetch_and_save_as_pdf(output_dir, repo_api_url):
    try:
        os.makedirs(output_dir, exist_ok=True)
        process_directory(output_dir, repo_api_url)
    except Exception as e:
        logging.error(f"Error fetching and saving as PDF: {e}")

if __name__ == "__main__":
    try:
        output_directory = f'{root_directory()}/data/flashbots_docs_pdf/'
        repo_api_url = "https://api.github.com/repos/flashbots/flashbots-docs/contents/docs"
        fetch_and_save_as_pdf(output_directory, repo_api_url)
    except Exception as e:
        logging.error(f"Error in main: {e}")
