import os

import arxiv
import requests
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from src.populate_csv_files.parse_pdf_files import parse_self_hosted_pdf
from src.utils import root_directory, read_csv_links_and_referrers, quickSoup, return_driver, create_directory, download_pdf, download_and_save_paper, validate_pdfs

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('arxiv')
logger.setLevel(logging.WARNING)


def get_paper_details_from_arxiv(arxiv_url: str) -> dict or None:
    """
       Retrieve paper details from Arxiv using its ID.

       Parameters:
       - arxiv_id (str): The ID of the paper on Arxiv.

       Returns:
       - dict: A dictionary containing details about the paper such as title, authors, pdf link, topics, and release date.
       - None: If there's an error during retrieval.
   """
    arxiv_id = arxiv_url.split('/')[-1]
    try:
        search = arxiv.Search(id_list=[arxiv_id])
        paper = next(search.results())

        # start list of paper.categories with 'arXiv'
        paper.categories = ['arXiv'] + paper.categories

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


def get_paper_details_from_ssrn(url: str) -> dict or None:
    """
    Retrieve paper details from an SSRN URL and download the paper if it doesn't exist locally.

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
        if not date:
            date = [line.replace("Posted: ", "") for line in test_list if "Posted: " in line]
        date = date[0].strip()
        original_date = datetime.strptime(date, '%d %b %Y')
        formatted_date = original_date.strftime('%Y-%m-%d')

        pdf_relative_link = article.select_one("div.abstract-buttons:nth-child(1) > div:nth-child(1) > a:nth-child(1)")['href']
        pdf_link = f"https://papers.ssrn.com/sol3/{pdf_relative_link}"

        # Check if the paper is already downloaded
        pdf_directory = os.path.join(root_directory(), 'data', 'papers_pdf_downloads')
        pdf_filename = f"{title}.pdf"
        pdf_path = os.path.join(pdf_directory, pdf_filename)

        # Download the paper if it doesn't exist locally
        if not os.path.exists(pdf_path):
            pdf_content = download_pdf(pdf_link, url)
            if pdf_content:
                with open(pdf_path, "wb") as f:
                    f.write(pdf_content)
            else:
                logging.warning(f"[SSRN] Failed to download a valid PDF file from {pdf_link}")

        details = {
            'title': title,
            'authors': authors,
            'pdf_link': pdf_link,
            'topics': 'SSRN',
            'release_date': formatted_date
        }
        logging.info(f"[SSRN] Successfully downloaded {title}")
        return details

    except Exception as e:
        logging.error(f"[SSRN] Failed to fetch details for {url}. Error: {e}")
        return None


def get_paper_details_from_iacr(url: str):
    try:
        response = requests.get(url.replace('.pdf', ''))
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # select the bibtex with css selector #bibtex
        # extract authors from such string template # author = {author1 and author2 and author3}

        paper_title = soup.select_one('head > title:nth-child(4)').get_text()

        bibtex = soup.select_one('#bibtex').get_text()
        # Extract authors using regular expression
        authors_match = re.findall(r'author = {(.*)}', bibtex)
        if authors_match:
            paper_authors = ', '.join([author.strip().replace('[', '').replace(']', '').replace("'", '') for author in authors_match[0].split(' and ')])
        else:
            paper_authors = ''

        paper_release_date = soup.select_one('#metadata > dl:nth-child(2) > dd:nth-child(12)').get_text().strip()
        if 'See all versions' in paper_release_date:
            paper_release_date = soup.select_one('#metadata > dl:nth-child(2) > dd:nth-child(10)').get_text().strip()
        if ':' in paper_release_date:
            paper_release_date = paper_release_date.split(':')[0].strip()

        # Check if the paper is already downloaded
        pdf_directory = os.path.join(root_directory(), 'data', 'papers_pdf_downloads')
        pdf_filename = f"{paper_title}.pdf"
        pdf_path = os.path.join(pdf_directory, pdf_filename)

        # Download the paper if it doesn't exist locally
        if not os.path.exists(pdf_path):
            pdf_content = download_pdf(f"{url}.pdf", url)
            if pdf_content:
                with open(pdf_path, "wb") as f:
                    f.write(pdf_content)
                logging.info(f"[IACR] Successfully downloaded [{paper_title}]")
            else:
                logging.warning(f"[IACR] Failed to download a valid PDF file from {url}")

        return {"title": paper_title.strip(), "authors": paper_authors.strip(), "pdf_link": url.strip(), "topics": 'iacr', "release_date": paper_release_date.strip()}
    except requests.exceptions.RequestException as e:
        logging.error(f"[IACR] Failed to fetch the paper details: {e}")
        return None
    except AttributeError as e:
        logging.error(f"[IACR] Failed to parse the paper details: {e}")
        return None


def get_paper_details_from_dl_acm(url: str):
    try:
        response = requests.get(url.replace('.pdf', ''))
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        paper_title = soup.select_one('.citation__title').get_text()

        # Get Paper Authors
        # Select all <li> elements with class "loa__item"
        author_items = soup.select('li.loa__item')

        # Initialize a list to store the extracted author names
        paper_authors = []

        # Loop through the author items
        for author_item in author_items:
            # Find the <img> element within the author item
            img_element = author_item.select_one('img.author-picture')

            # Extract the author name from the alt attribute of the <img> element
            if img_element:
                author_name = img_element.get_text()
                if author_name:
                    paper_authors.append(author_name)

        # join all authors in paper_authors in a single string separated with comma and space
        paper_authors = ', '.join(paper_authors)

        date_string = soup.select_one('.CitationCoverDate').get_text()

        # Parse the date string
        date_obj = datetime.strptime(date_string, '%d %B %Y')

        # Format the date as yyyy-mm-dd
        paper_release_date = date_obj.strftime('%Y-%m-%d')

        return {"title": paper_title, "authors": paper_authors, "pdf_link": url, "topics": 'DL-ACM', "release_date": paper_release_date}
    except requests.exceptions.RequestException as e:
        logging.error(f"[DL ACM] Failed to fetch the paper details: {e}")
        return None
    except AttributeError as e:
        logging.error(f"[DL ACM] Failed to parse the paper details: {e}")
        return None


def get_paper_details_from_nature(url: str):
    try:
        response = requests.get(url.replace('.pdf', ''))
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        try:
            article_identifier = soup.select_one('.c-article-identifiers__type').get_text()
        except AttributeError:
            article_identifier = soup.select_one('li.c-article-identifiers__item:nth-child(1)').get_text()

        if article_identifier == 'COMMENT':
            paper_title = soup.select_one('.c-article-magazine-title').get_text()

            # Select the .c-article-author-list element
            author_list_text = soup.select_one('.c-article-author-list').get_text()

            # Remove non-alphabet characters, split the text by commas, and clean up spaces
            paper_authors = ', '.join([re.sub(r'[^a-zA-Z, ]', '', author.replace('&', ',')).strip() for author in author_list_text.split(',')])

            paper_authors = re.sub(r'\s+', ' ', paper_authors)  # Replace multiple spaces with a single space

            # get published date
            date_string = soup.select_one('li.c-article-identifiers__item:nth-child(2) > time:nth-child(1)').get_text()
            date_obj = datetime.strptime(date_string, '%d %B %Y')  # Parse the date string
            paper_release_date = date_obj.strftime('%Y-%m-%d')  # Format the date as yyyy-mm-dd

        elif article_identifier == 'News & Views':
            paper_title = soup.select_one('.c-article-title').get_text()

            # Select the .c-article-author-list element
            author_list_text = soup.select_one('.c-article-author-list').get_text().split('\n')[0]

            # Remove non-alphabet characters, split the text by commas, and clean up spaces
            paper_authors = ', '.join([re.sub(r'[^a-zA-Z, ]', '', author.replace('&', ',')).strip() for author in author_list_text.split(',')])

            paper_authors = re.sub(r'\s+', ' ', paper_authors)  # Replace multiple spaces with a single space

            date_string = soup.select_one('li.c-article-identifiers__item:nth-child(2) > a:nth-child(1) > time:nth-child(1)').get_text()
            date_obj = datetime.strptime(date_string, '%d %B %Y')  # Parse the date string
            paper_release_date = date_obj.strftime('%Y-%m-%d')  # Format the date as yyyy-mm-dd

        return {"title": paper_title, "authors": paper_authors, "pdf_link": url, "topics": 'Nature', "release_date": paper_release_date}
    except requests.exceptions.RequestException as e:
        logging.error(f"[Nature] Failed to fetch the paper details: {e}")
        return None
    except AttributeError as e:
        print(f"Failed to parse the paper details: {e}")
        return None


def get_paper_details_from_research_gate(url: str):
    try:
        driver = return_driver()

        driver.get(url.replace('.pdf', ''))
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.nova-legacy-e-text')))

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        paper_title = soup.select_one('h1.nova-legacy-e-text').get_text()

        # Loop through the author list
        authors = soup.select('.nova-legacy-l-flex .research-detail-author-list__item a')
        paper_authors = [author.get_text().strip() for author in authors if author]
        # remove empty strings or only spaces
        paper_authors = [author for author in paper_authors if author.strip()]
        # remove duplicates
        paper_authors = list(dict.fromkeys(paper_authors))

        # Try some manual cleaning and see if that goes through
        paper_authors = [author for author in paper_authors if not re.search(r'university|institute|school', author, flags=re.IGNORECASE)]
        # join all authors in paper_authors in a single string separated with comma and space
        paper_authors = ', '.join(paper_authors)

        date_string = soup.select_one('div.nova-legacy-e-text--spacing-xxs:nth-child(1) > ul:nth-child(1) > li:nth-child(1)').get_text()

        # Parse the date string
        date_obj = datetime.strptime(date_string, '%B %Y')

        # Format the date as yyyy-mm-dd
        paper_release_date = date_obj.strftime('%Y-%m-%d')

        # TODO 2023-12-17: retrieve .pdf URL from researchGate else use selenium
        # Check if the paper is already downloaded
        # pdf_directory = os.path.join(root_directory(), 'data', 'papers_pdf_downloads')
        # pdf_filename = f"{paper_title}.pdf"
        # pdf_path = os.path.join(pdf_directory, pdf_filename)
#
        # # Download the paper if it doesn't exist locally
        # if not os.path.exists(pdf_path):
        #     pdf_content = download_pdf(f"{url}.pdf", url)
        #     if pdf_content:
        #         with open(pdf_path, "wb") as f:
        #             f.write(pdf_content)
        #         logging.info(f"[ResearchGate] Successfully downloaded [{paper_title}]")
        #     else:
        #         logging.warning(f"[ResearchGate] Failed to download a valid PDF file from {url}")


        return {"title": paper_title, "authors": paper_authors, "pdf_link": url, "topics": 'ResearchGate', "release_date": paper_release_date}
    except TimeoutException as e:
        print(f"Timeout waiting for page to load: {e}")
        return None
    except Exception as e:
        print(f"Failed to fetch or parse the paper details: {e}")
        return None
    finally:
        driver.quit()


def get_paper_details_from_sciendirect(url: str):
    try:
        driver = return_driver()

        driver.get(url.replace('.pdf', ''))
        WebDriverWait(driver, 10)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        paper_title = soup.select_one('.title-text').get_text()

        # Get Paper Authors
        # Select all <li> elements with class "loa__item"
        author_items = soup.select('.react-xocs-alternative-link')

        # Initialize a list to store the extracted author names
        author_names = []

        # Loop through the author items
        for author_item in author_items:
            # Find the <span> elements with class "given-name" and "text surname"
            given_name = author_item.select_one('.given-name').get_text()
            surname = author_item.select_one('.text.surname').get_text()

            # Concatenate the given name and surname without spaces
            full_name = f"{given_name} {surname}"

            # Append the full name to the list
            author_names.append(full_name)

        # Join all authors in author_names into a single string separated with spaces
        paper_authors = ' '.join(author_names)

        # Remove extra spaces and clean up the string
        paper_authors = ' '.join(paper_authors.split())  # Remove extra spaces

        date_string = soup.select_one('div.text-xs:nth-child(2)').get_text()

        # Define a regular expression pattern for "Month Year" format
        pattern = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b'

        # Search for the pattern in the input string
        match = re.search(pattern, date_string)

        if match:
            date_string = match.group()
        else:
            print("Date not found in the input string.")
            date_string = ''

        # Parse the date string
        date_obj = datetime.strptime(date_string, '%B %Y')

        # Format the date as yyyy-mm-dd
        paper_release_date = date_obj.strftime('%Y-%m-%d')

        return {"title": paper_title, "authors": paper_authors, "pdf_link": url, "topics": 'ScienceDirect', "release_date": paper_release_date}
    except requests.exceptions.RequestException as e:
        logging.error(f"[Science Direct] Failed to fetch the paper details: {e}")
        return None
    except AttributeError as e:
        print(f"Failed to parse the paper details: {e}")
        return None


def get_paper_details_from_semanticscholar(url: str):
    try:
        driver = return_driver()

        driver.get(url.replace('.pdf', ''))
        WebDriverWait(driver, 10)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        paper_title = soup.select_one('.fresh-paper-detail-page__header > h1:nth-child(2)').get_text()

        # Select the .c-article-author-list element
        author_list_text = soup.select_one('.author-list__link').get_text()

        # Remove non-alphabet characters, split the text by commas, and clean up spaces
        paper_authors = ', '.join([re.sub(r'[^a-zA-Z, ]', '', author.replace('&', ',')).strip() for author in author_list_text.split(',')])

        paper_authors = re.sub(r'\s+', ' ', paper_authors)  # Replace multiple spaces with a single space

        # get published date
        date_string = soup.select_one('li.paper-meta-item:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1)').get_text()
        date_obj = datetime.strptime(date_string, '%d %B %Y')  # Parse the date string
        paper_release_date = date_obj.strftime('%Y-%m-%d')  # Format the date as yyyy-mm-dd

        return {"title": paper_title, "authors": paper_authors, "pdf_link": url, "topics": 'SemanticScholar', "release_date": paper_release_date}
    except requests.exceptions.RequestException as e:
        logging.error(f"[Semantics Scholar] Failed to fetch the paper details: {e}")
        return None
    except AttributeError as e:
        print(f"Failed to parse the paper details: {e}")
        return None





# Main execution


def main():
    """
     Main execution script to download paper details and save them to a CSV file.

     This script does the following:
     1. Sets up directories for saving papers.
     2. Determines the path for the CSV file where paper details will be saved.
     3. Reads links of papers from various sources, fetches their details, and saves them to the CSV.

     Note:
     The script supports multiple paper sites, each with its own link file and parsing method.
     The links of papers to be processed should be organized in CSV files.
     The CSV file format for saving paper details includes columns for title, authors, PDF link, topics, release date, and referrer.
     """
    root_data_directory = os.path.join(root_directory(), 'data')
    papers_directory = os.path.join(root_data_directory, 'papers')
    create_directory(papers_directory)
    csv_file = os.path.join(root_data_directory, 'paper_details.csv')

    paper_sites = [
         {
             'name': 'arXiv',
             'link_file': 'arxiv_papers.csv',
             'parsing_method': get_paper_details_from_arxiv
         },
         {
             'name': 'SSRN',
             'link_file': 'ssrn_papers.csv',
             'parsing_method': get_paper_details_from_ssrn
         },
         {
             'name': 'iacr',
             'link_file': 'iacr_papers.csv',
             'parsing_method': get_paper_details_from_iacr
         },
         {
             'name': 'Nature',
             'link_file': 'nature_papers.csv',
             'parsing_method': get_paper_details_from_nature
         },
         {
             'name': 'Research Gate',
             'link_file': 'researchgate_papers.csv',
             'parsing_method': get_paper_details_from_research_gate
         },
         {
             'name': 'DL-ACM',
             'link_file': 'dl.acm_papers.csv',
             'parsing_method': get_paper_details_from_dl_acm
         },
        {
            'name': 'ScienceDirect',
            'link_file': 'sciencedirect_papers.csv',
            'parsing_method': get_paper_details_from_sciendirect
        },
        {
            'name': 'SemanticScholar',
            'link_file': 'semanticscholar_papers.csv',
            'parsing_method': get_paper_details_from_semanticscholar
        },
    ]

    for site in paper_sites:
        link_file_path = os.path.join(root_data_directory, 'links', 'research_papers', site['link_file'])
        links_and_referrers = read_csv_links_and_referrers(link_file_path)
        download_and_save_paper(site['name'], links_and_referrers, csv_file, site['parsing_method'])
    # OffHost
    parse_self_hosted_pdf()
    # Validate PDFs
    validate_pdfs(os.path.join(root_data_directory, 'papers_pdf_downloads'))


if __name__ == "__main__":
    main()
