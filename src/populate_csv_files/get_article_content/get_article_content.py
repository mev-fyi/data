import pandas as pd
import pdfkit
import logging
import os

from src.populate_csv_files.get_article_content.scrap_website import fetch_discourse_content_from_url, fetch_medium_content_from_url, fetch_mirror_content_from_url, fetch_frontier_tech_content_from_url, fetch_notion_content_from_url, fetch_hackmd_article_content, fetch_paradigm_article_content, fetch_propellerheads_article_content, fetch_jump_article_content, fetch_a16z_article_content, fetch_dba_article_content, fetch_iex_article_content, fetch_uniswap_article_content, fetch_substack_article_content, fetch_vitalik_article_content, fetch_monoceros_article_content, fetch_helius_article_content, fetch_mevio_article_content, fetch_outlierventures_article_content, fetch_gauntlet_article_content, fetch_chainlink_article_content, fetch_blocknative_article_content, fetch_shutter_article_content, fetch_duality_article_content, fetch_dydx_article_content, fetch_merkle_article_content, fetch_openzeppelin_article_content, fetch_zaryabs_article_content, empty_content, \
    fetch_cyfrin_article_content, fetch_nil_foundation_article_content, fetch_quillaudits_article_content, fetch_paragraph_xyz_article_content, fetch_brink_article_content
from src.populate_csv_files.get_article_content.utils import markdown_to_html
from src.populate_csv_files.get_article_content.get_flashbots_writings import fetch_flashbots_writing_contents_and_save_as_pdf
from src.utils import root_directory
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_content(row, output_dir):
    url = getattr(row, 'article')

    # Define a mapping of URL patterns to functions

    url_patterns = {
        'ethresear.ch': fetch_discourse_content_from_url,
        'collective.flashbots.net': fetch_discourse_content_from_url,
        'lido.fi': fetch_discourse_content_from_url,
        'research.lido.fi': fetch_discourse_content_from_url,
        'research.anoma': fetch_discourse_content_from_url,
        'research.arbitrum.io': fetch_discourse_content_from_url,
        'gov.uniswap.org': fetch_discourse_content_from_url,
        'governance.aave.com': fetch_discourse_content_from_url,
        'forum.celestia.org': fetch_discourse_content_from_url,
        'dydx.forum': fetch_discourse_content_from_url,
        'forum.arbitrum.foundation': fetch_discourse_content_from_url,
        'forum.aztec.network': fetch_discourse_content_from_url,
        'frontier.tech': fetch_frontier_tech_content_from_url,
        # 'vitalik.ca': fetch_vitalik_ca_article_content,  # TODO 2023-12-23
        'medium.com': fetch_medium_content_from_url,
        'blog.metrika': fetch_medium_content_from_url,
        'mirror.xyz': fetch_mirror_content_from_url,
        'iex.io': fetch_iex_article_content,
        'paradigm.xyz': fetch_paradigm_article_content,
        'hackmd.io': fetch_hackmd_article_content,
        'jumpcrypto.com': fetch_jump_article_content,
        'notion.site': fetch_notion_content_from_url,
        'notes.ethereum.org': fetch_hackmd_article_content,
        'dba.xyz': fetch_dba_article_content,
        'propellerheads.xyz': fetch_propellerheads_article_content,
        'a16z': fetch_a16z_article_content,
        'blog.uniswap': fetch_uniswap_article_content,
        'substack.com': fetch_substack_article_content,
        'vitalik.eth.limo': fetch_vitalik_article_content,
        # 'osmosis.zone': fetch_osmosis_article_content,
        'monoceros': fetch_monoceros_article_content,
        'www.helius.dev': fetch_helius_article_content,
        'mev.io': fetch_mevio_article_content,
        'outlierventures.io': fetch_outlierventures_article_content,
        'gauntlet.xyz': fetch_gauntlet_article_content,
        'blog.chain.link': fetch_chainlink_article_content,
        'blocknative.com': fetch_blocknative_article_content,
        'shutter.network': fetch_shutter_article_content,
        'duality.xyz': fetch_duality_article_content,
        'dydx.exchange': fetch_dydx_article_content,
        'merkle.io': fetch_merkle_article_content,
        'openzeppelin': fetch_openzeppelin_article_content,
        'blog.qtum': fetch_medium_content_from_url,
        'zaryabs': fetch_zaryabs_article_content,
        'cyfrin': fetch_cyfrin_article_content,
        'nil.foundation': fetch_nil_foundation_article_content,
        'quillaudits': fetch_quillaudits_article_content,
        'paragraph.xyz': fetch_paragraph_xyz_article_content,
        'brink': fetch_brink_article_content,
    }

    for pattern, fetch_function in url_patterns.items():
        if pattern in url:
            if fetch_function:
                content_info = fetch_function(url)
                return content_info
            else:
                logging.error(f"[fetch_content]: pattern is in URL but there is no fetch_function!!")

    # Default case if no match is found
    return empty_content


def update_csv(full_df, df, modified_indices, csv_filepath):
    """
    Update specific rows in the full DataFrame based on the modified subset DataFrame and save it to a CSV file.

    :param full_df: The original full DataFrame.
    :param df: The modified subset DataFrame.
    :param modified_indices: A list of indices in df corresponding to the rows modified.
    :param csv_filepath: File path of the CSV file to be updated.
    """
    for idx in modified_indices:
        # Identify the article URL in the modified subset DataFrame
        article_url = df.iloc[idx]['article']

        # Find the corresponding row in the full DataFrame based on article URL
        full_row_index = full_df[full_df['article'] == article_url].index

        # Update the corresponding row in full_df with the modified row in df
        if not full_row_index.empty:
            full_df.loc[full_row_index[0]] = df.iloc[idx]

    # Write the updated full DataFrame to CSV
    full_df.to_csv(csv_filepath, index=False)


def fetch_article_contents_and_save_as_pdf(csv_filepath, output_dir, num_articles=None, overwrite=True, url_filters=None, thread_count=None):
    """
    Fetch the contents of articles and save each content as a PDF in the specified directory.

    Parameters:
    - csv_filepath (str): The file path of the input CSV file containing article URLs and referrers.
    - output_dir (str): The directory where the article PDFs should be saved.
    - num_articles (int, optional): Number of articles to process. If None, process all articles.
    """
    # Read the entire CSV file into a DataFrame
    full_df = pd.read_csv(csv_filepath)
    if 'release_date' not in full_df.columns:
        full_df['release_date'] = None
    if 'authors' not in full_df.columns:
        full_df['authors'] = None

    # Create a filtered subset for processing
    df = full_df.copy()
    if url_filters is not None:
        df = df[df['article'].str.contains('|'.join(url_filters))]
    if num_articles is not None:
        df = df.head(num_articles)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # List to store indices of modified rows
    modified_indices = []

    # Function to process each row
    def process_row(row, index):
        nonlocal modified_indices
        article_url = getattr(row, 'article')
        article_title = getattr(row, 'title')

        import numpy as np
        if article_title is np.nan:
            article_title = article_url

        # Create a sanitized file name for the PDF from the article title
        pdf_filename = os.path.join(output_dir, article_title.replace("/", "<slash>") + '.pdf')

        # Check if PDF already exists
        if not os.path.exists(pdf_filename) or overwrite:
            content_info = fetch_content(row, output_dir)
            if content_info['content']:
                # Specify additional options for pdfkit to ensure UTF-8 encoding
                options = {
                    'encoding': "UTF-8",
                    'custom-header': [
                        ('Content-Encoding', 'utf-8'),
                    ],
                    'no-outline': None
                }
                if article_title == article_url:
                    # Create a sanitized file name for the PDF from the article title
                    if content_info['title']:
                        article_title = content_info['title']
                        df.loc[df['article'] == getattr(row, 'article'), 'title'] = article_title
                        pdf_filename = os.path.join(output_dir, article_title.replace("/", "<slash>") + '.pdf')

                pdfkit.from_string(markdown_to_html(content_info['content']), pdf_filename, options=options)
                # logging.info(f"Saved PDF [{article_title}] for {article_url}")

            else:
                # logging.warning(f"No content fetched for {article_url}")
                pass
                # Update the dataframe with the release date
            if content_info['release_date']:
                df.loc[df['title'] == getattr(row, 'title'), 'release_date'] = str(content_info['release_date'])
            if content_info['authors']:
                df.loc[df['title'] == getattr(row, 'title'), 'authors'] = str(content_info['authors'])

        # Update the modified rows list
        modified_indices.append(index)

    # Use ThreadPoolExecutor to process rows in parallel
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        executor.map(lambda pair: process_row(pair[1], pair[0]), enumerate(df.itertuples()))

    # Update only the modified rows in the full DataFrame
    update_csv(full_df, df, modified_indices, csv_filepath)


def add_new_articles():
    # Paths to the CSV files
    original_csv_file_path = os.path.join(root_directory(), 'data', 'links', 'articles.csv')
    updated_csv_file_path = os.path.join(root_directory(), 'data', 'links', 'articles_updated.csv')

    # Read the original and updated articles into DataFrames
    original_df = pd.read_csv(original_csv_file_path)
    updated_df = pd.read_csv(updated_csv_file_path)

    # Assuming 'article' is the column name that uniquely identifies rows
    # Identify new articles not present in the updated DataFrame
    new_articles = original_df[~original_df['article'].isin(updated_df['article'])]

    # Append new unique articles to the updated DataFrame and save
    if not new_articles.empty:
        # Use concat instead of append and drop duplicates just in case
        updated_df = pd.concat([updated_df, new_articles]).drop_duplicates(subset='article')
        # Save the updated DataFrame back to the updated CSV file
        updated_df.to_csv(updated_csv_file_path, index=False)


def run(url_filters=None, get_flashbots_writings=True, thread_count=None, overwrite=False):
    csv_file_path = f'{root_directory()}/data/links/articles_updated.csv'
    output_directory = f'{root_directory()}/data/articles_pdf_download/'
    add_new_articles()
    fetch_article_contents_and_save_as_pdf(csv_filepath=csv_file_path,
                                           output_dir=output_directory,
                                           overwrite=overwrite,
                                           url_filters=url_filters,
                                           thread_count=thread_count)
    if get_flashbots_writings:
        fetch_flashbots_writing_contents_and_save_as_pdf(output_directory)


if __name__ == "__main__":
    url_filters = ['nil.foundation']  # ['a16z']  # ['pbs']  # None # ['hackmd']
    thread_count = 1
    os.environ['NUMEXPR_MAX_THREADS'] = f'{thread_count}'
    get_flashbots_writings = False
    run(url_filters=url_filters, get_flashbots_writings=get_flashbots_writings, thread_count=thread_count, overwrite=True)
