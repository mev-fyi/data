import functools
import logging
import os
import pandas as pd
from urllib.parse import urlparse
import re

from src.utils import root_directory, ensure_newline_in_csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def add_domain_to_file(domain: str, filepath: str) -> None:
    with open(filepath, 'a') as file:
        file.write(f"{domain}\n")


def parse_and_categorize_links(input_filepath: str, domains_filepath: str, research_websites: list) -> None:
    df = pd.read_csv(input_filepath)

    domains = read_domains_from_file(domains_filepath)

    for index, row in df.iterrows():
        parsed_url = urlparse(row['paper'])
        domain = f"{parsed_url.scheme}://{parsed_url.netloc}"

        if domain not in research_websites and domain not in domains:
            sub_path = '/'.join(parsed_url.path.split('/')[:3])
            final_domain = f"{domain}{sub_path}"
            add_domain_to_file(final_domain, domains_filepath)
            domains.append(final_domain)


def categorize_url(url, url_patterns, existing_domains):
    """
    Categorize a URL based on specified patterns and existing domains.

    Args:
        url (str): The URL to categorize.
        url_patterns (dict): A dictionary of domain categories and their associated regex patterns.
        existing_domains (list): A list of existing domains to match against.

    Returns:
        str: The category of the URL, which can be "article," "website," "twitter thread," "video," or "unidentified."
    """
    if "twitter.com" in url and "/status/" in url:
        return "twitter thread"

    if ("https://www.youtube.com/watch" in url) or ("https://youtu.be/" in url) or ("https://www.youtube.com/playlist?list=" in url):
        return "video"

    for domain, pattern in url_patterns.items():
        if re.match(pattern, url):
            if "article" in domain:
                return "article"  # Categorize as an article
            elif "website" in domain:
                return "website"

    # Iterate through the list of existing domains
    for domain in existing_domains:
        # Check if the URL matches the existing domain using a case-insensitive regex match
        if re.match(fr'^{re.escape(domain)}', url, re.IGNORECASE):
            # If there is a match, categorize the URL as an "article" based on existing domains
            return "article"

    # Check if the URL matches the pattern for a website (https://<some_name>.<extension>/)
    if re.match(r'^https://[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+/$', url):
        return "website"

    return "unidentified"  # Default to categorizing as "unidentified"


def read_domains_from_file(filepath: str) -> list:
    # Read the CSV file and select the "website" column
    df = pd.read_csv(filepath)
    domains = df['website'].tolist()
    return domains


def parse_and_categorize_links(input_filepath: str, domains_filepath: str, research_websites: list, url_patterns) -> None:
    """
       Parse and categorize links from an input CSV file, applying various conditions and categorizations.

       Args:
           input_filepath (str): The file path of the input CSV containing links.
           domains_filepath (str): The file path of the domains file.
           research_websites (list): A list of research websites to consider.
           url_patterns (dict): A dictionary of URL patterns for categorization.

       Returns:
           None: The method performs operations on the input CSV and saves categorized data to output CSV files.
       """
    # Load your data
    df = pd.read_csv(input_filepath)

    # Remove rows containing "scihub" in the 'content' column
    scihub_mask = df['content'].str.contains('scihub', case=False)
    df = df[~scihub_mask]  # sorry we can't proceed with this one folks :(

    # Read the domains from the file
    domains = read_domains_from_file(domains_filepath)

    # Create separate DataFrames based on the conditions
    pdf_mask = df['content'].str.contains('.pdf', case=False) & ~df['content'].str.contains('arxiv|ssrn|iacr', case=False)
    research_masks = {site: df['content'].str.contains(site, case=False) for site in research_websites}
    arxiv_mask = df['content'].str.contains('arxiv', case=False)
    ssrn_mask = df['content'].str.contains('ssrn', case=False)
    iacr_mask = df['content'].str.contains('iacr', case=False)
    youtube_mask = df['content'].apply(lambda url: categorize_url(url, url_patterns, domains) == "video")

    # Create separate DataFrames based on the conditions
    # Assuming the 'content' column contains URLs, you can categorize them using the URL patterns
    articles_mask = df['content'].apply(lambda url: categorize_url(url, url_patterns, domains) == "article") & ~pdf_mask & ~arxiv_mask & ~ssrn_mask & ~iacr_mask & ~youtube_mask & ~functools.reduce(lambda x, y: x | y, research_masks.values())
    twitter_thread_mask = df['content'].apply(lambda url: categorize_url(url, url_patterns, domains) == "twitter thread")
    website_mask = df['content'].apply(lambda url: categorize_url(url, url_patterns, domains) == "website") & ~pdf_mask & ~arxiv_mask & ~ssrn_mask & ~iacr_mask & ~youtube_mask & ~functools.reduce(lambda x, y: x | y, research_masks.values())

    masks_list = [pdf_mask, arxiv_mask, ssrn_mask, iacr_mask, youtube_mask, articles_mask, twitter_thread_mask, website_mask] + list(research_masks.values())

    pdf_df = df[pdf_mask]
    arxiv_df = df[arxiv_mask]
    ssrn_df = df[ssrn_mask]
    iacr_df = df[iacr_mask]
    websites_df = df[website_mask]

    other_articles_mask = ~twitter_thread_mask

    # Create separate DataFrames for Twitter threads and other articles
    articles_df = df[articles_mask]
    twitter_thread_df = df[twitter_thread_mask]
    # other_articles_df = articles_df[other_articles_mask]

    # Creating a separate DataFrame for YouTube videos
    youtube_df = df[youtube_mask]

    # Creating a separate DataFrame for each research website
    research_dfs = {site: df[mask] for site, mask in research_masks.items()}

    # Create a mask that identifies rows to keep in the original DataFrame
    all_masks = masks_list
    keep_mask = ~(functools.reduce(lambda x, y: x | y, all_masks))

    # Apply the mask to keep only the rows that don't satisfy any of the conditions
    df = df[keep_mask]

    # Create the output directory if it does not exist
    repo_dir = root_directory()
    output_dir = f"{repo_dir}/data/links/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define file paths and dataframes in a dictionary
    paths_and_dfs = {
        "research_papers/papers.csv": (pdf_df, ["paper", "referrer"]),
        "research_papers/arxiv_papers.csv": (arxiv_df, ["paper", "referrer"]),
        "research_papers/ssrn_papers.csv": (ssrn_df, ["paper", "referrer"]),
        "research_papers/iacr_papers.csv": (iacr_df, ["paper", "referrer"]),
        "youtube/recommended_youtube_videos.csv": (youtube_df, ["video", "referrer"]),
        "articles.csv": (articles_df, ["article", "referrer"]),
        "twitter_threads.csv": (twitter_thread_df, ["twitter thread", "referrer"]),
        "websites.csv": (websites_df, ["website", "referrer"]),
    }

    # Correct column names for research_dfs
    research_columns = ["paper", "referrer"]

    # Update paths_and_dfs to include data frames from research_dfs
    paths_and_dfs.update({f"{site}_papers.csv": (rdf, research_columns) for site, rdf in research_dfs.items()})

    # Step 1: Loading existing data
    existing_data = {}
    for filename, (_, columns) in paths_and_dfs.items():
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            existing_data[filepath] = set(pd.read_csv(filepath)[columns[0]])

    # Step 2: Updating masks
    existing_in_csv_mask = pd.Series([False] * len(df))
    for filename, (new_df, columns) in paths_and_dfs.items():
        if not new_df.empty:  # Check if the DataFrame is not empty before saving it to a CSV
            filepath = os.path.join(output_dir, filename)
            if filepath in existing_data:
                existing_in_csv_mask |= df['content'].isin(existing_data[filepath])

                filepath = os.path.join(output_dir, filename)

                # Ensure CSV ends with a newline
                ensure_newline_in_csv(filepath)

                # Read the existing data
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    existing_df = pd.read_csv(filepath)
                else:
                    existing_df = pd.DataFrame(columns=columns)  # Add default columns based on the file being processed

                # Make new_df have the exact same columns as existing_df, in the same order
                # new_df = new_df.reindex(columns=existing_df.columns)
                new_df.columns = existing_df.columns

                # Concat new and existing data and remove duplicates based on the first column
                combined_df = pd.concat([existing_df, new_df])
                combined_df.drop_duplicates(subset=columns[0], keep='first', inplace=True)

                # Debug prints to understand the data
                logging.info(f"New data for {filepath}: {len(new_df)} rows")
                logging.info(f"Combined data for {filepath}: {len(combined_df)} rows")
                # for each filepath print all new data content
                for item in new_df.iloc[0].to_list():
                    logging.info(f"New data for {filepath}: {item}\n\n")

                # Save the non-duplicate data back to the CSV
                combined_df.to_csv(filepath, index=False)

    # Step 3: Updating the keep_mask
    keep_mask &= ~existing_in_csv_mask

    # Apply the updated keep_mask
    df = df[keep_mask]

    # Save the modified original DataFrame back to the input CSV file only if the script is successful
    df.to_csv(input_filepath, index=False)


def run():
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
        "flashbots_article": r"^https://collective\.flashbots\.net/t/.+/\d+$",
        "url_vitalik_article": r"^https://vitalik\.ca/.+/.+/.+\.html$",
        "vitalik_website": r"^https://vitalik\.ca/$",
        "url_qje_article": r"^https://academic\.oup\.com/qje/article/.+",
        "hackmd_article": r"^https://hackmd\.io/[^/]+/[^/]+$",  # New pattern for hackmd.io URLs
        "flashbots_article_2": r"^https://collective\.flashbots\.net/t/.+/\d+$",  # New pattern for flashbots.net URLs
        "scrt_network_website": r"^https://scrt\.network/$",  # Match https://scrt.network/
        "scrt_network_blog_website": r"^https://scrt\.network/blog$",  # Match https://scrt.network/blog
        "scrt_network_article": r"^https://scrt\.network/blog/.+$",  # Match https://scrt.network/blog/<something>
        "github_website": r"^https://github\.com/[^/]+/$",  # Match https://github.com/<something>/
        "github_article": r"^https://github\.com/20squares/[^/]+$",  # Match https://github.com/20squares/<something>
        "medium_article2": r"^https://medium\.com/[^/]+/[^/]+$",  # Match https://medium.com/<something>/<something>
        "medium_website2": r"^https://medium\.com/[^/]+$",  # Match https://medium.com/<something>
        "flashbots_article_writing": r"^https://writings\.flashbots\.net/.+$",  # Match https://writings.flashbots.net/<something>
        "flashbots_website": r"^https://writings\.flashbots\.net/$",  # Match https://writings.flashbots.net/
        "iex_article": r"^https://www\.iex\.io/article/.+$",  # Match https://www.iex.io/article/<something>
        "iex_website": r"^https://www\.iex\.io/article/$",  # Match https://www.iex.io/article/
        "iexexchange_website": r"^https://www\.iexexchange\.io/technology$",  # Match https://www.iexexchange.io/technology
        "paradigm_article": r"^https://www\.paradigm\.xyz/.+$",  # Match https://www.paradigm.xyz/<something>
        "paradigm_website": r"^https://www\.paradigm\.xyz/$",  # Match https://www.paradigm.xyz/
        "flashbots_docs_article": r"^https://docs\.flashbots\.net/.+$",  # New pattern for Flashbots docs
        "flashbots_docs_website": r"^https://docs\.flashbots\.net/$",  # New pattern for Flashbots docs website
        "mirror_xyz_article_2": r"^https://mirror\.xyz/[^/]+/[^/]+$",  # Pattern for articles under mirror.xyz
        "mirror_xyz_website_2": r"^https://mirror\.xyz/[^/]+/$",  # Pattern for the base mirror.xyz website
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


# Main execution
if __name__ == "__main__":
    run()
