import pandas as pd
from PyPDF2 import PdfReader
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import fitz  # PyMuPDF
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
import pikepdf

from src.utils import root_directory


# Step 3 & 4: Iterating over each row and trying to get the PDF details
def get_pdf_details(url):
    try:
        response = requests.get(url)
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
                    print(f"Could not retrieve details with [PyPDF] from {url}: {e}")
            if not paper_title:  # If details retrieval still fails, try with PyMuPDF
                try:
                    f.seek(0)
                    doc = fitz.open(f)
                    info = doc.metadata
                    paper_title = info['title']
                except Exception as e:
                    print(f"Could not retrieve details with [PyMuPDF] from {url}: {e}")

            if not paper_title:  # If details retrieval still fails, try with pdfminer
                try:
                    f.seek(0)
                    parser = PDFParser(f)
                    doc = PDFDocument(parser)
                    info = doc.info[0]
                    paper_title = info['Title'].decode('utf-8', 'ignore') if 'Title' in info else ''
                except Exception as e:
                    print(f"Could not retrieve details with [pdfminer] from {url}: {e}")

            if not paper_title:  # If details retrieval still fails, try with pikepdf
                try:
                    f.seek(0)
                    doc = pikepdf.open(f)
                    info = doc.open_metadata()
                    paper_title = info['Title'] if 'Title' in info else ''
                except Exception as e:
                    print(f"Could not retrieve details with [pikepdf] from {url}: {e}")

            # Extract creation date and format it to "yyyy-mm-dd"
            print('\n', info)
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

            # Creating and returning the details dictionary
            details = {
                "title": paper_title,
                "authors": paper_authors,
                "pdf_link": url,
                "topics": 'Self-host',
                "release_date": paper_release_date
            }
            print(f"Retrieved: {details}\n\n")
    except Exception as e:
        # Creating and returning the details dictionary
        details = {
            "title": None,
            "authors": None,
            "pdf_link": url,
            "topics": 'Self-host',
            "release_date": None
        }
        print(f"Could not retrieve details from {url}: {e}")
    return details


def parse_self_hosted_pdf():
    # Load the existing data into a pandas DataFrame
    existing_data_filepath = f'{root_directory()}/data/paper_details.csv'
    existing_df = pd.read_csv(existing_data_filepath)

    # Step 2: Load the CSV file with the links into a pandas DataFrame
    df = pd.read_csv(f'{root_directory()}/data/links/research_papers/papers.csv')

    # Find the rows in df where the PDF link is not already present in existing_df
    unique_entries = ~df['paper'].isin(existing_df['pdf_link'])

    # Filter df to only include these unique rows
    df = df[unique_entries]

    if df.empty:
        print("No new entries to process.")
        return
    else:
        print(f"Processing {len(df)} new entries...")

    # Preserve the referrer column for later use
    referrer_series = df['referrer'].copy()

    # Step 6: Storing the details in a new column in the DataFrame
    # Creating a ThreadPoolExecutor to parallelize the operation
    with ThreadPoolExecutor() as executor:
        df['paper_details'] = list(executor.map(get_pdf_details, df['paper']))

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


parse_self_hosted_pdf()

