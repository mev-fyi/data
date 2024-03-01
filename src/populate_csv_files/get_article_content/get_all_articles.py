import pandas as pd
import pdfkit
import logging
import os
from src.populate_csv_files.get_article_content.scrap_website import fetch_discourse_content_from_url
from src.populate_csv_files.get_article_content.utils import markdown_to_html
from src.utils import root_directory
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_single_csv(file, csv_dir, output_dir):
    df = pd.read_csv(os.path.join(csv_dir, file))
    output_subdir = os.path.join(output_dir, file.split('.')[0])
    os.makedirs(output_subdir, exist_ok=True)

    for index, row in df.iterrows():
        fetch_and_save_pdf(row, output_subdir)


def fetch_and_save_pdf(row, output_dir):
    sanitized_title = row['Title'].replace('/', '_').replace('"', "'")
    pdf_filename = os.path.join(output_dir, f"{sanitized_title}.pdf")

    # Check if PDF already exists
    if not os.path.exists(pdf_filename):
        content_info = fetch_discourse_content_from_url(row['Link'])
        if content_info['content']:
            pdfkit.from_string(markdown_to_html(content_info['content']), pdf_filename)
            logging.info(f"PDF saved: [{'/'.join(pdf_filename.split('/')[-2:])}]")
        else:
            logging.error(f"Failed to fetch content for URL: {row['Link']}")
    else:
        pass
        logging.info(f"PDF already exists: [{'/'.join(pdf_filename.split('/')[-2:])}]")


def process_csv_files_in_parallel(csv_dir, output_dir, thread_count):
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    final_output_dir = os.path.join(output_dir, 'all_discourse_topics')
    os.makedirs(final_output_dir, exist_ok=True)

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        executor.map(process_single_csv, csv_files, [csv_dir] * len(csv_files), [final_output_dir] * len(csv_files))


if __name__ == "__main__":
    csv_directory = f"{root_directory()}/data/links/articles"
    output_directory = f"{root_directory()}/data/articles_pdf_download"
    thread_count = 20  # Adjust this to the desired number of parallel file processors

    os.environ['NUMEXPR_MAX_THREADS'] = str(thread_count)
    process_csv_files_in_parallel(csv_directory, output_directory, thread_count)
