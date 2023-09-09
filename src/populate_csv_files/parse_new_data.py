import functools
import logging
import os
import pandas as pd
from urllib.parse import urlparse
import re

from src.utils import root_directory, ensure_newline_in_csv


def add_domain_to_file(domain: str, filepath: str) -> None:
    with open(filepath, 'a') as file:
        file.write(f"{domain}\n")


def parse_and_categorize_links(input_filepath: str, domains_filepath: str, research_websites: list) -> None:
    df = pd.read_csv(input_filepath)

    domains = read_domains_from_file(domains_filepath)

    for index, row in df.iterrows():
        parsed_url = urlparse(row['paper'])
        domain = f"{parsed_url.scheme}://{parsed_url.netloc}"

        if domain not in research_websites and domain not in domains and parsed_url.path.startswith('/insights/research/'):
            sub_path = '/'.join(parsed_url.path.split('/')[:3])
            final_domain = f"{domain}{sub_path}"
            add_domain_to_file(final_domain, domains_filepath)
            domains.append(final_domain)


def categorize_url(url, url_patterns, existing_domains):
    if "twitter.com" in url and "/status/" in url:
        return "Twitter thread"

    for domain, pattern in url_patterns.items():
        if re.match(pattern, url):
            return "article"  # Categorize as an article

    for domain in existing_domains:
        if re.match(fr'^{re.escape(domain)}', url, re.IGNORECASE):
            return "article"  # Categorize as an article based on existing domains

    return "website"  # Default to categorizing as a website


def read_domains_from_file(filepath: str) -> list:
    with open(filepath, 'r') as file:
        domains = [line.strip() for line in file if line.strip()]
    return domains


# TODO 2023-09-08: spin off parse_and_categorize_links from utils.py to standalone script
def parse_and_categorize_links(input_filepath: str, domains_filepath: str, research_websites: list, url_patterns) -> None:
    # Load your data
    df = pd.read_csv(input_filepath)

    # Read the domains from the file
    domains = read_domains_from_file(domains_filepath)

    # Create separate DataFrames based on the conditions
    pdf_mask = df['paper'].str.contains('.pdf', case=False) & ~df['paper'].str.contains('arxiv|ssrn|iacr', case=False)
    research_masks = {site: df['paper'].str.contains(site, case=False) for site in research_websites}
    arxiv_mask = df['paper'].str.contains('arxiv', case=False)
    ssrn_mask = df['paper'].str.contains('ssrn', case=False)
    iacr_mask = df['paper'].str.contains('iacr', case=False)
    youtube_mask = df['paper'].str.startswith('https://www.youtube.com/watch') | df['paper'].str.startswith('https://youtu.be/')


    # Create separate DataFrames based on the conditions
    articles_mask = df['paper'].apply(lambda url: categorize_url(url, url_patterns, domains) == "article")

    website_mask = ~articles_mask

    pdf_df = df[pdf_mask]
    arxiv_df = df[arxiv_mask]
    ssrn_df = df[ssrn_mask]
    iacr_df = df[iacr_mask]
    articles_df = df[articles_mask]
    websites_df = df[website_mask]

    # Separate filtering for Twitter threads and other articles
    twitter_thread_mask = articles_df['paper'].apply(lambda url: "twitter.com" in url and "/status/" in url)
    other_articles_mask = ~twitter_thread_mask

    # Create separate DataFrames for Twitter threads and other articles
    twitter_thread_df = articles_df[twitter_thread_mask]
    other_articles_df = articles_df[other_articles_mask]

    # Creating a separate DataFrame for YouTube videos
    youtube_df = df[youtube_mask]

    # Creating a separate DataFrame for each research website
    research_dfs = {site: df[mask] for site, mask in research_masks.items()}

    # Create a mask that identifies rows to keep in the original DataFrame
    all_masks = [pdf_mask, youtube_mask] + list(research_masks.values())
    keep_mask = ~(functools.reduce(lambda x, y: x | y, all_masks) | articles_mask)

    # Apply the mask to keep only the rows that don't satisfy any of the conditions
    df = df[keep_mask]

    # Create the output directory if it does not exist
    repo_dir = root_directory()
    output_dir = f"{repo_dir}/data/links/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define file paths and dataframes in a dictionary
    paths_and_dfs = {
        "papers.csv": pdf_df,
        "arxiv_papers.csv": arxiv_df,
        "ssrn_papers.csv": ssrn_df,
        "iacr_papers.csv": iacr_df,
        "recommended_youtube_videos.csv": youtube_df,
        "articles.csv": articles_df,
        "twitter_threads.csv": twitter_thread_df,
        "websites.csv": websites_df,
    }

    # Update paths_and_dfs to include data frames from research_dfs
    paths_and_dfs.update({f"{site}_papers.csv": rdf for site, rdf in research_dfs.items()})

    # Save the DataFrames to separate CSV files
    for filename, new_df in paths_and_dfs.items():
        if not new_df.empty:  # Check if the DataFrame is not empty before saving it to a CSV
            filepath = os.path.join(output_dir, filename)

            # Ensure CSV ends with a newline
            ensure_newline_in_csv(filepath)

            # Read the existing data
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                existing_df = pd.read_csv(filepath)
            else:
                existing_df = pd.DataFrame(columns=["paper", "referrer"])  # Add default columns

            # Concat new and existing data and remove duplicates based on the 'paper' column
            combined_df = pd.concat([existing_df, new_df])
            combined_df.drop_duplicates(subset=['paper'], keep='first', inplace=True)

            # Debug prints to understand the data
            logging.info(f"New data for {filepath}: {len(new_df)} rows")
            logging.info(f"Combined data for {filepath}: {len(combined_df)} rows")

            # Save the non-duplicate data back to the CSV
            combined_df.to_csv(filepath, index=False)

    # Save the modified original DataFrame back to the input CSV file only if the script is successful
    df.to_csv(input_filepath, index=False)


# Main execution
if __name__ == "__main__":
    repo_dir = root_directory()

    url_patterns = {
        "notion_site": r"^https://[^/]+/[^/]+$",
        "medium_article": r"^https://\w+\.medium\.com/.+",
        "blog_metrika_article": r"^https://blog\.metrika\.co/.+",
        "mirror_xyz_article_1": r"^https://\w+\.mirror\.xyz/.+",
        "mirror_xyz_website": r"^https://mirror\.xyz/.+",
        "drive_google_article": r"^https://drive\.google\.com/file/d/.+",
        "galaxy_insights_article_1": r"^https://www\.galaxy\.com/insights/.+/.+",
        "galaxy_insights_website": r"^https://www\.galaxy\.com/insights/.+",
        "ethereum_notes_website": r"^https://notes\.ethereum\.org/@[^/]+/$",
        "ethereum_notes_article": r"^https://notes\.ethereum\.org/@[^/]+/[^/]+$",
        "medium_article_2": r"^https://medium\.com/.+/.+",
        "medium_website": r"^https://medium\.com/.+",
        "twitter_website": r"^https://twitter\.com/[^/]+/$",
        "twitter_thread": r"^https://twitter\.com/bertcmiller/status/.+",
        "flashbots_article": r"^https://collective\.flashbots\.net/t/.+/\d+$",
        "url_vitalik_article": r"^https://vitalik\.ca/.+/.+/.+\.html$",
        "vitalik_website": r"^https://vitalik\.ca/$",
        "url_qje_article": r"^https://academic\.oup\.com/qje/article/.+",
    }
    # TODO 2023-09-08: fix the website writing to .csv logic.
    parse_and_categorize_links(
        input_filepath=os.path.join(repo_dir, "data/links/to_parse.csv"),
        domains_filepath=os.path.join(repo_dir, "data/links/websites.csv"),
        research_websites=[
            'arxiv', 'ssrn', 'iacr', 'pubmed', 'ieeexplore', 'springer',
            'sciencedirect', 'dl.acm', 'jstor', 'nature', 'researchgate',
            'scholar.google', 'semanticscholar'
        ],
        url_patterns=url_patterns
    )
