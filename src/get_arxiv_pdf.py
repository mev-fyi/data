import os
import arxiv
import requests
import csv
from bs4 import BeautifulSoup
import concurrent.futures


def get_paper_details_from_arxiv_id(arxiv_id):
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
        print(f"Failed to fetch details for {arxiv_id}. Error: {e}")
        return None


def read_existing_papers(csv_file):
    """Read existing papers from a CSV file."""
    existing_papers = []
    if os.path.exists(csv_file):
        with open(csv_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_papers.append(row['title'])
    return existing_papers


def paper_exists_in_list(title, existing_papers):
    """Check if the paper title exists in the list of existing papers."""
    return title in existing_papers


def download_and_save_paper(link, csv_file, existing_papers, headers):
    arxiv_id = link.split('/')[-1].replace('.pdf', '')
    paper_details = get_paper_details_from_arxiv_id(arxiv_id)

    if paper_details is None:
        print(f"Failed to fetch details for {arxiv_id}. Skipping...")
        return

    # Check if paper exists in CSV
    if paper_exists_in_list(paper_details['title'], existing_papers):
        print(f"Arxiv paper with title '{paper_details['title']}' already exists in the CSV. Skipping...")
        return

    # Append to CSV
    with open(csv_file, 'a', newline='') as csvfile:  # open in append mode to write data
        fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date']
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
    print(f"Downloaded Arxiv paper {pdf_filename}")


def download_arxiv_papers(paper_links, csv_file):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    existing_papers = read_existing_papers(csv_file)

    # Write header only if CSV file is empty (newly created)
    if not existing_papers:
        with open(csv_file, 'w', newline='') as csvfile:  # open in write mode only to write the header
            fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    # Use ProcessPoolExecutor to run tasks in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(download_and_save_paper, paper_links, [csv_file]*len(paper_links), [existing_papers]*len(paper_links), [headers]*len(paper_links))


def root_directory():
    current_dir = os.getcwd()

    while True:
        if '.git' in os.listdir(current_dir):
            return current_dir
        else:
            # Go up one level
            current_dir = os.path.dirname(current_dir)


def quickSoup(url):
    try:
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        soup = BeautifulSoup(requests.get(url, headers=header, timeout=10).content, 'html.parser')
        return soup
    except Exception:
        return None


def get_ssrn_details_from_url(url):
    try:
        article = quickSoup(url)
        t = article.get_text()
        if "The abstract you requested was not found" in t:
            return None  # Return None for articles that aren't found

        def ordered_set_from_list(input_list):
            return list(dict.fromkeys(input_list).keys())

        title = article.find('h1').get_text().replace("\n", "").strip()
        test_list = ordered_set_from_list(t.split("\n"))
        authors = test_list[0].replace(title, "").replace(" :: SSRN", "").replace(" by ", "").replace(", ", ":").strip()
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
        print(f"Failed to fetch details for {url}. Error: {e}")
        return None


def paper_exists_in_csv(title, csv_file):
    try:
        with open(csv_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['title'] == title:
                    return True
    except FileNotFoundError:
        return False
    return False


def download_and_save_ssrn_paper(link, csv_file):
    paper_details = get_ssrn_details_from_url(link)
    if paper_details is None:
        print(f"Failed to fetch details for {link}. Skipping...")
        return

    # Check if paper exists in CSV
    if paper_exists_in_csv(paper_details['title'], csv_file):
        print(f"SSRN paper with title '{paper_details['title']}' already exists in the CSV. Skipping...")
        return

    # Write to CSV
    with open(csv_file, 'a', newline='') as csvfile:  # Open in append mode to write data
        fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(paper_details)
    print(f"Added details for SSRN paper titled '{paper_details['title']}' to CSV.")


def download_ssrn_papers(paper_links, csv_file):
    # Use ProcessPoolExecutor to run tasks in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(download_and_save_ssrn_paper, paper_links, [csv_file]*len(paper_links))


# Main execution
if __name__ == "__main__":
    papers_directory = os.path.join(root_directory(), 'data', 'papers')
    os.makedirs(papers_directory, exist_ok=True)

    csv_file = os.path.join(root_directory(), 'data', 'paper_details.csv')

    # For arXiv links
    with open(os.path.join(root_directory(), 'data', 'links', 'arxiv_papers.txt'), 'r') as f:
        arxiv_links = [line.strip() for line in f.readlines()]
    download_arxiv_papers(paper_links=arxiv_links, csv_file=csv_file)

    # For SSRN links. Credits to https://github.com/karthiktadepalli1/ssrn-scraper
    # Assuming you have a file of SSRN links similar to arXiv
    with open(os.path.join(root_directory(), 'data', 'links', 'ssrn_papers.txt'), 'r') as f:
        ssrn_links = [line.strip() for line in f.readlines()]
    download_ssrn_papers(paper_links=ssrn_links, csv_file=csv_file)

