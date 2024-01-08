import requests
import os
import pdfkit
import logging
import time
from markdown import markdown
from src.utils import root_directory
import dotenv
from concurrent.futures import ThreadPoolExecutor
from weasyprint import HTML


# Load environment variables
dotenv.load_dotenv()
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_content_list(github_api_url, headers, retry_count=3, delay=5):
    try:
        response = requests.get(github_api_url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403 and 'rate limit exceeded' in e.response.text:
            if retry_count > 0:
                logging.warning(f"Rate limit exceeded. Retrying in {delay} seconds...")
                time.sleep(delay)
                return get_content_list(github_api_url, headers, retry_count-1, delay*2)
        logging.error(f"Error fetching content from {github_api_url}: {e}")
        return []

def process_directory(output_dir, api_url, headers, overwrite=True):
    contents = get_content_list(api_url, headers)
    with ThreadPoolExecutor() as executor:
        futures = []
        for content in contents:
            if content['type'] == 'file' and content['name'].endswith(('.md', '.mdx')):
                future = executor.submit(process_file, output_dir, content, headers, overwrite)
                futures.append(future)
            elif content['type'] == 'dir':
                new_output_dir = os.path.join(output_dir, content['name'])
                new_api_url = content['url']
                os.makedirs(new_output_dir, exist_ok=True)
                process_directory(new_output_dir, new_api_url, headers, overwrite)  # Nested directories are processed sequentially

        # Wait for all futures to complete
        for future in futures:
            future.result()


def process_file(output_dir, file_info, headers, overwrite):
    file_name = file_info['name']
    output_pdf_path = os.path.join(output_dir, file_name.replace('.md', '.pdf').replace('.mdx', '.pdf'))

    if not overwrite and os.path.exists(output_pdf_path):
        logging.info(f"File already exists: {output_pdf_path}. Skipping download and conversion.")
        return

    try:
        file_url = file_info['download_url']
        file_content = requests.get(file_url, headers=headers).text
        convert_to_pdf_with_fallback(file_content, output_pdf_path)
        logging.info(f"Successfully processed and saved: {output_pdf_path}")
    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")

def convert_to_pdf_with_fallback(markdown_content, output_pdf_path, retry_count=3, delay=5):
    # First try using pdfkit
    try:
        html_content = '<meta charset="UTF-8">' + markdown(markdown_content, extensions=['md_in_html'])
        options = {
            'encoding': "UTF-8",
            'custom-header': [('Content-Encoding', 'utf-8')],
            'no-outline': None
        }
        pdfkit.from_string(html_content, output_pdf_path, options=options)
        return True  # Successfully converted using pdfkit
    except Exception as e:
        logging.warning(f"pdfkit failed to convert to PDF: {e}. Trying WeasyPrint...")

    # If pdfkit fails, try using WeasyPrint
    try:
        HTML(string=html_content).write_pdf(output_pdf_path)
        return True  # Successfully converted using WeasyPrint
    except Exception as e:
        if retry_count > 0:
            logging.warning(f"WeasyPrint also failed to convert to PDF: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            return convert_to_pdf_with_fallback(markdown_content, output_pdf_path, retry_count-1, delay*2)

    logging.error(f"Failed to convert to PDF after retries: {output_pdf_path}")
    return None  # Failed conversion in all attempts


def fetch_and_save_as_pdf(output_dir, repo_api_url, headers, overwrite=True):
    try:
        os.makedirs(output_dir, exist_ok=True)
        process_directory(output_dir, repo_api_url, headers, overwrite)
    except Exception as e:
        logging.error(f"Error fetching and saving as PDF: {e}")

def process_repositories(overwrite=True):
    headers = {'Authorization': f'token {GITHUB_TOKEN}'} if GITHUB_TOKEN else {}
    if GITHUB_TOKEN:
        logging.info("Github token found! Authenticated to increase number of get requests")
    else:
        logging.warning('Github token not provided, defaulting to unauthenticated get requests')

    # Flashbots Docs
    # flashbots_output_dir = f'{root_directory()}/data/flashbots_docs_pdf/'
    # flashbots_repo_api_url = "https://api.github.com/repos/flashbots/flashbots-docs/contents/docs"
    # fetch_and_save_as_pdf(flashbots_output_dir, flashbots_repo_api_url)

    # Ethereum Org Website
    ethereum_output_dir = f'{root_directory()}/data/ethereum_org_website_content/'
    ethereum_repo_api_url = "https://api.github.com/repos/ethereum/ethereum-org-website/contents/src/content"
    fetch_and_save_as_pdf(ethereum_output_dir, ethereum_repo_api_url, headers, overwrite)

if __name__ == "__main__":
    try:
        process_repositories(overwrite=False) # Change to False to avoid overwriting
    except Exception as e:
        logging.error(f"Error processing repositories: {e}")
