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


def extract_authors_from_mdx(mdx_content, default_authors='Flashbots'):
    try:
        lines = mdx_content.split('\n')
        if lines[0] == '---':
            end_of_front_matter = lines.index('---', 1)
            front_matter = '\n'.join(lines[1:end_of_front_matter])
            front_matter_yaml = yaml.safe_load(front_matter)
            authors = str(front_matter_yaml.get('authors', default_authors))

            # Process each author to create the link
            author_links = [
                f"""https://collective.flashbots.net/u/{author.strip("[]").replace("'", "")}"""
                for author in authors.split(', ')
            ]

            # Join the links with ", "
            return ', '.join(author_links)
    except Exception as e:
        logging.error(f"Error extracting authors: {e}")

    return default_authors


def convert_mdx_to_pdf(mdx_content, output_pdf_path):
    try:
        html_content = '<meta charset="UTF-8">' + markdown.markdown(mdx_content)
        options = {
            'encoding': "UTF-8",
            'custom-header': [('Content-Encoding', 'utf-8')],
            'no-outline': None
        }
        pdfkit.from_string(html_content, output_pdf_path, options=options)
    except Exception as e:
        logging.error(f"Error converting MDX to PDF: {e}")


def process_file_info(output_dir, file_info):
    if file_info['name'].endswith('.mdx'):
        mdx_url = file_info['download_url']
        mdx_content = requests.get(mdx_url).text
        default_title = os.path.splitext(file_info['name'])[0]
        title = extract_title_from_mdx(mdx_content, default_title)
        authors = extract_authors_from_mdx(mdx_content)
        file_name = title + '.pdf'
        output_pdf_path = os.path.join(output_dir, file_name)
        convert_mdx_to_pdf(mdx_content, output_pdf_path)
        logging.info(f"Processed {file_name}")

        # Extract the release date from the filename
        try:
            release_date = datetime.strptime(file_info['name'][:10], '%Y-%m-%d').date()
        except ValueError:
            release_date = None  # or use a default date if required
        return title, release_date, authors


def fetch_flashbots_writing_contents_and_save_as_pdf(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    flashbots_writings_file_list = get_file_list()

    # Initialize a list to store data before converting to DataFrame
    articles_data = []

    with ThreadPoolExecutor() as executor:
        for result in executor.map(lambda file_info: process_file_info(output_dir, file_info), flashbots_writings_file_list):
            if result:
                # Append the result tuple to the list
                articles_data.append({'title': result[0], 'release_date': result[1], 'authors': result[2]})

    # Convert the list to a DataFrame
    articles_df = pd.DataFrame(articles_data)

    # Update the CSV file
    csv_path = f'{root_directory()}/data/links/articles_updated.csv'
    existing_df = pd.read_csv(csv_path)

    # Check if the 'release_date' column exists, if not, add it
    if 'release_date' not in existing_df.columns:
        existing_df['release_date'] = None

    if 'authors' not in existing_df.columns:
        existing_df['authors'] = None

    # Update the existing DataFrame with the new release dates
    for idx, row in articles_df.iterrows():
        existing_df.loc[existing_df['title'] == row['title'], 'release_date'] = row['release_date']
        existing_df.loc[existing_df['title'] == row['title'], 'authors'] = row['authors']

    # Save the updated DataFrame
    existing_df.to_csv(csv_path, index=False)


if __name__ == "__main__":
    # Call the function
    output_directory = f'{root_directory()}/data/articles_pdf_download/'
    fetch_flashbots_writing_contents_and_save_as_pdf(output_directory)
