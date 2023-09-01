import os
import arxiv
import requests
import csv


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


def download_arxiv_papers(paper_links, csv_file):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    with open(csv_file, 'w', newline='') as csvfile:
        fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date']  # added 'release_date' here
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for link in paper_links:
            arxiv_id = link.split('/')[-1]
            arxiv_id = arxiv_id.replace('.pdf', '')

            paper_details = get_paper_details_from_arxiv_id(arxiv_id)
            if paper_details is None:
                print(f"Failed to fetch details for {arxiv_id}. Skipping...")
                continue

            # Write to CSV
            writer.writerow(paper_details)

            # Download the PDF
            pdf_response = requests.get(paper_details['pdf_link'], headers=headers)
            pdf_response.raise_for_status()

            # Save the PDF locally with the desired naming scheme
            pdf_filename = f"{arxiv_id}_{paper_details['title']}.pdf"
            pdf_path = os.path.join(root_directory(), 'data', 'papers', pdf_filename)
            with open(pdf_path, 'wb') as f:
                f.write(pdf_response.content)
            print(f"Downloaded {pdf_filename}")


def root_directory():
    current_dir = os.getcwd()

    while True:
        if '.git' in os.listdir(current_dir):
            return current_dir
        else:
            # Go up one level
            current_dir = os.path.dirname(current_dir)


if __name__ == "__main__":
    papers_directory = os.path.join(root_directory(), 'data', 'papers')
    os.makedirs(papers_directory, exist_ok=True)

    csv_file = os.path.join(root_directory(), 'data', 'paper_details.csv')
    with open(os.path.join(root_directory(), 'data', 'links', 'papers.txt'), 'r') as f:
        lines = f.readlines()

    # Remove any newline characters from each line
    paper_links = [line.strip() for line in lines]

    download_arxiv_papers(paper_links=paper_links, csv_file=csv_file)