import pandas as pd
import pdfkit
import logging
import os
from src.populate_csv_files.get_article_content.scrap_website import fetch_discourse_content_from_url
from src.populate_csv_files.get_article_content.utils import markdown_to_html
from src.utils import root_directory
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def merge_csv_files(csv_dir, output_csv_path):
    # Get a list of all CSV files in the specified directory
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

    # Helper function to read a CSV file and add a column with the file's name
    def process_csv(file):
        df = pd.read_csv(os.path.join(csv_dir, file))
        df['Source CSV'] = file
        return df

    # Process all CSV files in parallel and collect their DataFrames
    with ThreadPoolExecutor() as executor:
        dataframes = list(executor.map(process_csv, csv_files))

    # Concatenate all DataFrames into a single DataFrame
    merged_df = pd.concat(dataframes, ignore_index=True)

    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_csv_path, index=False)


def fetch_and_save_pdf(row, output_dir):
    content_info = fetch_discourse_content_from_url(row['Link'])
    if content_info['content']:
        pdf_filename = os.path.join(output_dir, f"{row['Title'].replace('/', '_')}.pdf")
        pdfkit.from_string(markdown_to_html(content_info['content']), pdf_filename)
        logging.info(f"PDF saved: [{pdf_filename}]")
    else:
        logging.error(f"Failed to fetch content for URL: {row['Link']}")


def process_csv_files_in_parallel(csv_filepath, output_dir, thread_count):
    df = pd.read_csv(csv_filepath)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        executor.map(fetch_and_save_pdf, [row for _, row in df.iterrows()], [output_dir] * len(df))


if __name__ == "__main__":
    csv_directory = f"{root_directory()}/data/links/articles"
    merged_csv_file = f"{root_directory()}/data/links/merged_articles.csv"
    output_directory = f"{root_directory()}/data/articles_pdf_download/"

    merge_csv_files(csv_directory, merged_csv_file)
    thread_count = 10  # Set this to the number of threads you want to use.
    os.environ['NUMEXPR_MAX_THREADS'] = str(thread_count)

    process_csv_files_in_parallel(merged_csv_file, output_directory, thread_count)
