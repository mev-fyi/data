import os
import arxiv
import requests
import csv
from bs4 import BeautifulSoup
import concurrent.futures
import logging
from dotenv import load_dotenv

from src.upload_google_sheet import update_google_sheet_with_csv
from src.utils import root_directory, ensure_newline_in_csv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('arxiv')
logger.setLevel(logging.WARNING)


def read_csv_links_and_referrers(file_path):
    with open(file_path, mode='r') as f:
        reader = csv.DictReader(f)
        return [(row['paper'], row['referrer']) for row in reader]


def get_paper_details_from_arxiv_id(arxiv_id: str) -> dict or None:
    """
       Retrieve paper details from Arxiv using its ID.

       Parameters:
       - arxiv_id (str): The ID of the paper on Arxiv.

       Returns:
       - dict: A dictionary containing details about the paper such as title, authors, pdf link, topics, and release date.
       - None: If there's an error during retrieval.
   """
    try:
        search = arxiv.Search(id_list=[arxiv_id])
        paper = next(search.results())

        details = {
            'title': paper.title,
            'authors': ", ".join([author.name for author in paper.authors]),
            'pdf_link': paper.pdf_url,
            'topics': ", ".join(paper.categories),
            'release_date': paper.published.strftime('%Y-%m-%d')  # formatting date to 'YYYY-MM-DD' string format
        }
        return details
    except Exception as e:
        logging.error(f"Failed to fetch details for {arxiv_id}. Error: {e}")
        return None


def read_existing_papers(csv_file: str) -> list:
    """
    Read paper titles from a given CSV file.

    Parameters:
    - csv_file (str): Path to the CSV file.

    Returns:
    - list: A list containing titles of the papers from the CSV.
    """
    existing_papers = []
    if os.path.exists(csv_file):
        with open(csv_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_papers.append(row['title'])
    return existing_papers


def paper_exists_in_list(title: str, existing_papers: list) -> bool:
    """
    Check if a paper title already exists in a list of existing papers.

    Parameters:
    - title (str): The title of the paper.
    - existing_papers (list): List of existing paper titles.

    Returns:
    - bool: True if title exists in the list, False otherwise.
    """
    return title in existing_papers


def download_and_save_arxiv_paper(link: str, csv_file: str, existing_papers: list, headers: dict, referrer: str) -> None:
    """
    Download a paper from its link and save its details to a CSV file.

    Parameters:
    - link (str): Direct link to the paper's PDF.
    - csv_file (str): Path to the CSV file.
    - existing_papers (list): List of existing paper titles.
    - headers (dict): Headers for the request.
    """
    arxiv_id = link.split('/')[-1].replace('.pdf', '')
    paper_details = get_paper_details_from_arxiv_id(arxiv_id)

    if paper_details is None:
        logging.error(f"Failed to fetch details for {arxiv_id}")
        return

    # Check if paper exists in CSV
    if paper_exists_in_list(paper_details['title'], existing_papers):
        logging.info(f"Arxiv paper with title '{paper_details['title']}' already exists in the CSV. Skipping...")
        return

    # Ensure CSV ends with a newline
    ensure_newline_in_csv(csv_file)

    paper_details["referrer"] = referrer

    # Append to CSV
    with open(csv_file, 'a', newline='') as csvfile:  # open in append mode to write data
        fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date', 'referrer']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(paper_details)

    # Download the PDF
    pdf_response = requests.get(paper_details['pdf_link'], headers=headers)
    pdf_response.raise_for_status()

    # Save the PDF locally
    pdf_filename = f"{arxiv_id}_{paper_details['title']}.pdf"
    pdf_path = os.path.join(root_directory(), 'data', 'papers', pdf_filename)
    with open(pdf_path, 'wb') as f:
        f.write(pdf_response.content)
    logging.info(f"Downloaded Arxiv paper {pdf_filename}")


def download_arxiv_papers(paper_links_and_referrers: list, csv_file: str) -> None:
    """
    Download multiple papers from Arxiv and save their details to a CSV file.

    Parameters:
    - paper_links_and_referrers (list): List of links and their referrers to the papers.
    - csv_file (str): Path to the CSV file.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    existing_papers = read_existing_papers(csv_file)

    # Write header only if CSV file is empty (newly created)
    if not existing_papers:
        with open(csv_file, 'w', newline='') as csvfile:  # open in write mode only to write the header
            fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date', 'referrer']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    # Use ProcessPoolExecutor to run tasks in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(
            download_and_save_arxiv_paper,
            [link for link, referrer in paper_links_and_referrers],
            [csv_file] * len(paper_links_and_referrers),
            [existing_papers] * len(paper_links_and_referrers),
            [headers] * len(paper_links_and_referrers),
            [referrer for link, referrer in paper_links_and_referrers]
        )


def quickSoup(url) -> BeautifulSoup or None:
    """
    Quickly retrieve and parse an HTML page into a BeautifulSoup object.

    Parameters:
    - url (str): The URL of the page to be fetched.

    Returns:
    - BeautifulSoup object: Parsed HTML of the page.
    - None: If there's an error during retrieval.
    """
    try:
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        soup = BeautifulSoup(requests.get(url, headers=header, timeout=10).content, 'html.parser')
        return soup
    except Exception:
        return None


def get_ssrn_details_from_url(url: str) -> dict or None:
    """
    Retrieve paper details from an SSRN URL.

    Parameters:
    - url (str): The URL of the SSRN paper.

    Returns:
    - dict: A dictionary containing details about the paper.
    - None: If there's an error during retrieval or the abstract is not found.
    """
    try:
        article = quickSoup(url)
        t = article.get_text()
        if "The abstract you requested was not found" in t:
            return None  # Return None for articles that aren't found

        def ordered_set_from_list(input_list):
            return list(dict.fromkeys(input_list).keys())

        title = article.find('h1').get_text().replace("\n", "").strip()
        test_list = ordered_set_from_list(t.split("\n"))
        authors = test_list[1].replace(title, "").replace(" :: SSRN", "").replace(" by ", "").replace(", ", ":").strip().replace(':', ', ')
        date = [line.replace("Last revised: ", "") for line in test_list if "Last revised: " in line]

        # Fallback if "Last revised" isn't found
        if not date:
            date = [line.replace("Posted: ", "") for line in test_list if "Posted: " in line]

        # Extract the date
        date = date[0].strip()

        details = {
            'title': title,
            'authors': authors,
            'pdf_link': url,
            'topics': 'SSRN',
            'release_date': date  # Extracted date from SSRN
        }
        return details

    except Exception as e:
        logging.error(f"Failed to fetch details for {url}. Error: {e}")
        return None


def paper_exists_in_csv(title: str, csv_file: str) -> bool:
    """
    Check if a paper title already exists in a given CSV file.

    Parameters:
    - title (str): The title of the paper.
    - csv_file (str): Path to the CSV file.

    Returns:
    - bool: True if title exists in the CSV, False otherwise.
    """

    try:
        with open(csv_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['title'] == title:
                    return True
    except FileNotFoundError:
        return False
    return False


def reference_and_log_ssrn_paper(link: str, csv_file: str, referrer: str) -> None:
    """
    Download a paper's details from its SSRN link and save to a CSV file.

    Parameters:
    - link (str): URL link to the SSRN paper.
    - csv_file (str): Path to the CSV file.
    """
    paper_details = get_ssrn_details_from_url(link)
    if paper_details is None:
        logging.warning(f"Failed to fetch details for {link}. Skipping...")
        return

    # Check if paper exists in CSV
    if paper_exists_in_csv(paper_details['title'], csv_file):
        logging.info(f"SSRN paper with title '{paper_details['title']}' already exists in the CSV. Skipping...")
        return

    # Ensure CSV ends with a newline
    ensure_newline_in_csv(csv_file)

    paper_details["referrer"] = referrer

    # Write to CSV
    with open(csv_file, 'a', newline='') as csvfile:  # Open in append mode to write data
        fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date', 'referrer']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(paper_details)
    logging.info(f"Added details for SSRN paper titled '{paper_details['title']}' to CSV.")


def reference_and_log_ssrn_papers(paper_links_and_referrers: list, csv_file: str) -> None:
    """
    Download multiple papers' details from SSRN and save them to a CSV file.

    Parameters:
    - paper_links_and_referrers (list): List of tuples with links to the SSRN papers and their referrers.
    - csv_file (str): Path to the CSV file.
    """
    # Use ProcessPoolExecutor to run tasks in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Unpack links and referrers for the executor
        paper_links, referrers = zip(*paper_links_and_referrers)
        executor.map(reference_and_log_ssrn_paper, paper_links, [csv_file] * len(paper_links), referrers)


# Main execution
if __name__ == "__main__":
    """
    Main execution script to download paper details and save them to a CSV file.

    This script does the following:
    1. Sets up directories for saving papers.
    2. Determines the path for the CSV file where paper details will be saved.
    3. Reads links of arXiv papers from a text file, fetches their details, and saves them to the CSV.
    4. Reads links of SSRN papers from a text file, fetches their details, and saves them to the CSV.

    Note: 
    The SSRN scraping logic credits to https://github.com/karthiktadepalli1/ssrn-scraper.
    It assumes a file of SSRN links similar to the arXiv links file is present.
    """
    papers_directory = os.path.join(root_directory(), 'data', 'papers')
    os.makedirs(papers_directory, exist_ok=True)

    csv_file = os.path.join(root_directory(), 'data', 'paper_details.csv')

    if os.getenv("FETCH_NEW_PDF") == "True":
        # For arXiv links
        arxiv_links_and_referrers = read_csv_links_and_referrers(os.path.join(root_directory(), 'data', 'links', 'arxiv_papers.csv'))
        download_arxiv_papers(paper_links_and_referrers=arxiv_links_and_referrers, csv_file=csv_file)

        # For SSRN links. Credits to https://github.com/karthiktadepalli1/ssrn-scraper
        ssrn_links_and_referrers = read_csv_links_and_referrers(os.path.join(root_directory(), 'data', 'links', 'ssrn_papers.csv'))
        reference_and_log_ssrn_papers(paper_links_and_referrers=ssrn_links_and_referrers, csv_file=csv_file)

    # Update the Google Sheet after updating the CSV
    update_google_sheet_with_csv(csv_file=csv_file, sheet_id=os.getenv("GOOGLE_SHEET_ID"))


