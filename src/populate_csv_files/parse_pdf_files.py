import logging
import os
from functools import partial
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from PyPDF2 import PdfReader
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import fitz  # PyMuPDF
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
import pikepdf

from src.populate_csv_files.get_article_content.get_article_content import update_csv
from src.utils import root_directory, download_pdf


def load_failed_urls():
    failed_url_path = f'{root_directory()}/data/failed_urls.csv'
    if Path(failed_url_path).exists():
        with open(failed_url_path, 'r') as f:
            return set(f.read().splitlines())
    return set()


def save_failed_url(url):
    failed_url_path = f'{root_directory()}/data/failed_urls.csv'
    with open(failed_url_path, 'a') as f:
        f.write(url + '\n')


def download_self_hosted_pdfs(df, url, paper_title):
    # Try fetching the paper title from the DataFrame
    paper_title_row = df[df['pdf_link'] == url]
    if paper_title_row['title'].iloc[0] != np.nan:
        paper_title_from_df = paper_title_row['title'].iloc[0] if not paper_title_row.empty else str(urlparse(url).path.split('/')[-1]).replace('.pdf', '').replace('%20', ' ')
        paper_title = paper_title_from_df

    pdf_directory = os.path.join(root_directory(), 'data', 'papers_pdf_downloads')
    pdf_filename = f"{paper_title.replace('/', '<slash>').replace('.pdf', '').replace('docx', '').replace('Microsoft Word - ', '')}"
    pdf_path = os.path.join(pdf_directory, f"{pdf_filename}.pdf")

    # Download the paper if it doesn't exist locally
    pdf_content = download_pdf(f"{url}", url)
    if pdf_content:
        with open(pdf_path, "wb") as f:
            f.write(pdf_content)
        logging.info(f"[Self-host] Successfully downloaded [{paper_title}]")
    else:
        logging.warning(f"[Self-host] Failed to download a valid PDF file from {url}")


# Step 3 & 4: Iterating over each row and trying to get the PDF details
def get_pdf_details(url, df):
    paper_title = None
    try:
        # Attempt to download the PDF content
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Step 5: Using PyPDF2 to get the PDF details
        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            info = reader.metadata

            paper_title = info.title
            if info.title:  # Try getting details with PyPDF2
                try:
                    paper_title = info.title
                except Exception as e:
                    logging.info(f"Could not retrieve [{paper_title}] details with [PyPDF] from {url}: {e}")
            if not paper_title:  # If details retrieval still fails, try with PyMuPDF
                try:
                    f.seek(0)
                    doc = fitz.open(f)
                    info = doc.metadata
                    paper_title = info['title']
                except Exception as e:
                    logging.info(f"Could not retrieve [{paper_title}] details with [PyMuPDF] from {url}: {e}")

            if not paper_title:  # If details retrieval still fails, try with pdfminer
                try:
                    f.seek(0)
                    parser = PDFParser(f)
                    doc = PDFDocument(parser)
                    info = doc.info[0]
                    paper_title = info['Title'].decode('utf-8', 'ignore') if 'Title' in info else ''
                except Exception as e:
                    logging.info(f"Could not retrieve details [{paper_title}] with [pdfminer] from {url}: {e}")

            if not paper_title:  # If details retrieval still fails, try with pikepdf
                try:
                    f.seek(0)
                    doc = pikepdf.open(f)
                    info = doc.open_metadata()
                    paper_title = info['Title'] if 'Title' in info else ''
                except Exception as e:
                    logging.info(f"Could not retrieve details [{paper_title}] with [pikepdf] from {url}: {e}")

            # Extract creation date and format it to "yyyy-mm-dd"
            # print('\n', info)
            # if paper_title:
            f.seek(0)
            reader = PdfReader(f)
            info = reader.metadata
            creation_date_str = info['/CreationDate']
            year = creation_date_str[2:6]
            month = creation_date_str[6:8]
            day = creation_date_str[8:10]
            paper_release_date = f"{year}-{month}-{day}"

            # Setting placeholder values for authors and topics
            try:
                paper_authors = str(info['/Author'])
            except KeyError:
                paper_authors = str(info['/Creator'])
            if 'and' in paper_authors:
                paper_authors = paper_authors.replace(' and', ', ')

            # check using regex if 'adobe' or 'acrobat' or 'apache' or 'version' or 'tex' or 'context' in paper_authors then make it None
            if 'adobe' or 'acrobat' or 'apache' or 'version' or 'tex' or 'context' in paper_authors.lower():
                paper_authors = None

            paper_title = paper_title.strip()
            download_self_hosted_pdfs(df, url, paper_title)

            # Creating and returning the details dictionary
            details = {
                "title": paper_title,
                "authors": paper_authors,
                "pdf_link": url,
                "topics": 'Self-host',
                "release_date": paper_release_date
            }
            # print(f"Retrieved: {details}\n\n")
    except requests.exceptions.RequestException as e:
        logging.warning(f"Network error for {url}: {e}")
        save_failed_url(url)
        return {'title': None, 'authors': None, 'pdf_link': url, 'topics': 'Self-host', 'release_date': None}
    except Exception as e:
        logging.error(f"[get_pdf_details] Unexpected error for {url}: {e}")
        save_failed_url(url)
        return {'title': None, 'authors': None, 'pdf_link': url, 'topics': 'Self-host', 'release_date': None}
    return details


def parse_self_hosted_pdf(overwrite=False):
    existing_data_filepath = f'{root_directory()}/data/paper_details.csv'
    existing_df = pd.read_csv(existing_data_filepath)

    failed_urls = load_failed_urls()
    df = pd.read_csv(f'{root_directory()}/data/links/research_papers/papers.csv')

    if not overwrite:
        df = df[~df['paper'].isin(failed_urls)]
        df = df[~df['paper'].isin(existing_df['pdf_link'])]

    if df.empty:
        logging.info("No new entries to process.")
        return
    else:
        logging.info(f"Processing {len(df)} new entries...")

    referrer_series = df['referrer'].copy()

    get_pdf_details_partial = partial(get_pdf_details, df=existing_df)

    with ThreadPoolExecutor() as executor:
        df['paper_details'] = list(executor.map(get_pdf_details_partial, df['paper']))

    # Add the referrer series to the DataFrame
    df['referrer'] = referrer_series

    # Expand the 'paper_details' dictionaries into separate columns
    df_paper_details_expanded = df['paper_details'].apply(pd.Series)
    df = pd.concat([df, df_paper_details_expanded], axis=1)

    # Drop the original 'paper_details' and 'paper' columns as they are now redundant
    df.drop(columns=['paper_details', 'paper'], inplace=True)

    # Rearrange the columns as per your specified order
    df = df[['title', 'authors', 'pdf_link', 'topics', 'release_date', 'referrer']]

    # Display the DataFrame with the retrieved details
    print(df)

    # Load the existing data from paper_details.csv into a new DataFrame
    existing_data_filepath = f'{root_directory()}/data/paper_details.csv'
    existing_df = pd.read_csv(existing_data_filepath)

    # Find the rows in df where the PDF link is not already present in existing_df
    unique_entries = ~df['pdf_link'].isin(existing_df['pdf_link'])

    # Filter df to only include these unique rows
    df_unique = df[unique_entries]

    # Concatenate the existing DataFrame with the unique rows of the new DataFrame
    final_df = pd.concat([existing_df, df_unique], ignore_index=True)

    # Save the concatenated DataFrame back to the CSV file
    final_df.to_csv(existing_data_filepath, index=False)

    print(f"The DataFrame has been saved to {existing_data_filepath}")


if __name__ == "__main__":
    parse_self_hosted_pdf(overwrite=True)

