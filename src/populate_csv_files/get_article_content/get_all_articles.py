import pandas as pd
import pdfkit
import logging
import os
from src.populate_csv_files.get_article_content.scrap_website import fetch_discourse_content_from_url
from src.populate_csv_files.get_article_content.utils import markdown_to_html
from src.utils import root_directory
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_single_csv(file_path, output_dir, overwrite=False):
    file_name = os.path.basename(file_path)
    logging.info(f"Started processing [{file_name}]")
    df = pd.read_csv(file_path)
    output_subdir = os.path.join(output_dir, file_name.split('.')[0])
    os.makedirs(output_subdir, exist_ok=True)

    for index, row in df.iterrows():
        fetch_and_save_pdf(row, output_subdir, overwrite)

    logging.info(f"Finished processing [{file_name}]")


def fetch_and_save_pdf(row, output_dir, overwrite=False):
    sanitized_title = row['Title'].replace('/', '_').replace('"', "'")
    pdf_filename = os.path.join(output_dir, f"{sanitized_title}.pdf")

    if not os.path.exists(pdf_filename) or overwrite:
        content_info = fetch_discourse_content_from_url(row['Link'])
        if content_info['content']:
            pdfkit.from_string(markdown_to_html(content_info['content']), pdf_filename)
            logging.info(f"PDF saved: [{'/'.join(pdf_filename.split('/')[-2:])}]")
        else:
            logging.error(f"Failed to fetch content for URL: {row['Link']}")
    else:
        logging.info(f"PDF already exists: [{'/'.join(pdf_filename.split('/')[-2:])}]")


def process_csv_files_in_parallel(csv_dir_or_file, output_dir, thread_count, overwrite=False):
    if os.path.isfile(csv_dir_or_file):
        # Single CSV file processing
        csv_files = [csv_dir_or_file]
    else:
        # Multiple CSV files processing
        csv_files = [os.path.join(csv_dir_or_file, f) for f in os.listdir(csv_dir_or_file) if f.endswith('.csv')]

    final_output_dir = os.path.join(output_dir, 'all_discourse_topics')
    os.makedirs(final_output_dir, exist_ok=True)

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        executor.map(process_single_csv, csv_files, [final_output_dir] * len(csv_files), [overwrite] * len(csv_files))


if __name__ == "__main__":
    # csv_path_or_directory = f"{root_directory()}/data/links/articles"  # Can be a directory or a single CSV file
    csv_path_or_directory = f"{root_directory()}/data/links/articles/ethresearch_links.csv"  # Can be a directory or a single CSV file
    output_directory = f"{root_directory()}/data/articles_pdf_download"
    thread_count = 20  # Adjust based on your system capabilities

    os.environ['NUMEXPR_MAX_THREADS'] = str(thread_count)
    process_csv_files_in_parallel(csv_path_or_directory, output_directory, thread_count, overwrite=True)
