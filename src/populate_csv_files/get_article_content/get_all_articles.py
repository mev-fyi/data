import pandas as pd
import pdfkit
import logging
import os
from src.populate_csv_files.get_article_content.scrap_website import fetch_discourse_content_from_url
from src.populate_csv_files.get_article_content.utils import markdown_to_html
from src.utils import root_directory
from concurrent.futures import ThreadPoolExecutor
import time
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_urls_in_parallel(df, output_subdir, overwrite=False, thread_count=20):
    """
    Process each URL in the dataframe in parallel.
    """
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(fetch_and_save_pdf, row, output_subdir, overwrite) for _, row in df.iterrows()]
        for future in futures:
            future.result()


def fetch_and_save_pdf(row, output_dir, overwrite=False):
    sanitized_title = row['Title'].replace('/', '_').replace('"', "'")
    pdf_filename = os.path.join(output_dir, f"{sanitized_title}.pdf")

    if not os.path.exists(pdf_filename) or overwrite:
        time.sleep(random.randint(1, 5))
        content_info = fetch_discourse_content_from_url(row['Link'])
        try:
            if content_info['content']:
                pdfkit.from_string(markdown_to_html(content_info['content']), pdf_filename)
                logging.info(f"PDF saved: [{'/'.join(pdf_filename.split('/')[-2:])}]")
            else:
                logging.error(f"Failed to fetch content for URL: {row['Link']}")
        except Exception as e:
            logging.error(f"Error saving PDF {row}: {e}, continuing")
    else:
        logging.info(f"PDF already exists: [{'/'.join(pdf_filename.split('/')[-2:])}]")


def process_single_csv(file_path, output_dir, overwrite=False, thread_count=20):
    file_name = os.path.basename(file_path)
    logging.info(f"Started processing [{file_name}]")
    df = pd.read_csv(file_path)
    output_subdir = os.path.join(output_dir, file_name.split('.')[0])
    os.makedirs(output_subdir, exist_ok=True)

    process_urls_in_parallel(df, output_subdir, overwrite, thread_count)

    logging.info(f"Finished processing [{file_name}]")


def process_csv_files_in_parallel(csv_dir_or_file, output_dir, thread_count, overwrite=False):
    final_output_dir = os.path.join(output_dir, 'all_discourse_topics')
    os.makedirs(final_output_dir, exist_ok=True)

    if os.path.isfile(csv_dir_or_file):
        # Process a single CSV file in parallel over its URLs
        process_single_csv(csv_dir_or_file, final_output_dir, overwrite, thread_count)
    else:
        # Process each CSV file in the directory sequentially
        csv_files = [os.path.join(csv_dir_or_file, f) for f in os.listdir(csv_dir_or_file) if f.endswith('.csv')]
        for csv_file in csv_files:
            process_single_csv(csv_file, final_output_dir, overwrite, thread_count)


def merge_csv_files_remove_duplicates_and_save(csv_directory=f"{root_directory()}/data/links/articles", output_csv_path=f"{root_directory()}/data/links/merged_articles.csv"):
    """
    Concatenates all CSV files in the given directory, removes duplicates based on the 'Link' column,
    and saves the resulting DataFrame to the specified output path.

    Args:
        csv_directory (str): Directory containing CSV files to merge.
        output_csv_path (str): Path to save the merged and deduplicated CSV file.
    """
    # List all CSV files in the directory
    csv_files = [os.path.join(csv_directory, f) for f in os.listdir(csv_directory) if f.endswith('.csv')]
    df_list = []

    # Load and concatenate all CSV files
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        df_list.append(df)

    if df_list:
        merged_df = pd.concat(df_list, ignore_index=True)

        # Remove duplicates based on 'Link' column
        deduplicated_df = merged_df.drop_duplicates(subset=['Link'])

        # Save the resulting DataFrame to CSV
        deduplicated_df.to_csv(output_csv_path, index=False)
        logging.info(f"Merged and deduplicated CSV saved to: {output_csv_path}")
    else:
        logging.warning("No CSV files found in the provided directory.")


def run(overwrite=False):
    csv_path_or_directory = f"{root_directory()}/data/links/articles"  # Can be a directory or a single CSV file
    output_directory = f"{root_directory()}/data/articles_pdf_download"
    thread_count = 20  # Adjust based on your system capabilities

    os.environ['NUMEXPR_MAX_THREADS'] = str(thread_count)
    process_csv_files_in_parallel(csv_path_or_directory, output_directory, thread_count, overwrite=overwrite)


if __name__ == "__main__":
    # csv_path_or_directory = f"{root_directory()}/data/links/articles"  # Can be a directory or a single CSV file
    csv_path_or_directory = f"{root_directory()}/data/links/articles/ethresearch.csv"  # Adjust as needed
    output_directory = f"{root_directory()}/data/articles_pdf_download"
    thread_count = 20  # Adjust based on your system capabilities

    os.environ['NUMEXPR_MAX_THREADS'] = str(thread_count)

    process_csv_files_in_parallel(csv_path_or_directory, output_directory, thread_count, overwrite=True)

