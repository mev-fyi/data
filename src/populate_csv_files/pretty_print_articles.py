import time
import random
import logging

import pandas as pd
from bs4 import BeautifulSoup
import requests
from src.utils import root_directory, return_driver
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver


def fetch_title_from_url(url, css_selector):
    """
    Fetch the title of an article from a URL using the specified CSS selector to locate the title in the HTML.

    Parameters:
    - url (str): The URL of the article.
    - css_selector (str): The CSS selector to locate the title in the HTML.

    Returns:
    - str: The title of the article, or None if the title could not be fetched.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        title = soup.select_one(css_selector).text.strip()
        logging.info(f"Fetched title [{title}] for URL {url}")
        return title
    except Exception as e:
        logging.info(f"Could not fetch title for URL {url}: {e}")
        return None


def fetch_discourse_titles(url):
    return fetch_title_from_url(url, 'title')


def fetch_frontier_tech_titles(url):
    return fetch_title_from_url(url, 'head > title:nth-child(4)')


def fetch_flashbots_writings_titles(url):
    return fetch_title_from_url(url, '.blogPostTitle_RC3s')


def fetch_mirror_titles(url):
    return fetch_title_from_url(url, 'head > title:nth-child(7)')


def fetch_iex_titles(url):
    return fetch_title_from_url(url, '.header-content-title')


def fetch_paradigm_titles(url):
    return fetch_title_from_url(url, '.Post_post__J7vh4 > h1:nth-child(1)')


def fetch_jump_titles(url):
    return fetch_title_from_url(url, 'h1.MuiTypography-root')


def fetch_propellerheads_titles(url):
    return fetch_title_from_url(url, '.article-title_heading')


def fetch_a16z_titles(url):
    return fetch_title_from_url(url, '.highlight-display > h2:nth-child(1)')


def fetch_uniswap_titles(url):
    return fetch_title_from_url(url, '.p.Type__Title-sc-ga2v53-2:nth-child(1)')


def fetch_osmosis_titles(url):
    return fetch_title_from_url(url, '.article-head > h1:nth-child(1)')


def fetch_mechanism_titles(url):
    return fetch_title_from_url(url, '.heading-7')


def fetch_notion_titles(url):
    """
    Fetch the title of a notion.site page using Selenium to handle dynamic JavaScript content.

    Parameters:
    - url (str): The URL of the notion.site page.

    Returns:
    - str: The title of the page, or None if the title could not be fetched.
    """
    driver = return_driver()

    try:
        driver.get(url)

        # Wait for some time to allow JavaScript to load content
        driver.implicitly_wait(10)  # Adjust the wait time as necessary
        time.sleep(5)

        # Get the page title
        title = driver.title
        logging.info(f"Fetched title [{title}] for URL {url}")
        return title
    except Exception as e:
        logging.info(f"Could not fetch title for URL {url}: {e}")
        return None
    finally:
        # Always close the browser to clean up
        driver.quit()


def fetch_hackmd_titles(url):
    """
    Fetch the title of an article from a HackMD URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - str: The title of the article, or None if the title could not be fetched.
    """
    title = None
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        title_element = soup.find('title')
        if title_element:
            title = title_element.get_text()
            logging.info(f"Fetched title [{title}] for URL {url}")
        else:
            logging.info(f"Title not found for URL {url}")
    except Exception as e:
        logging.info(f"Could not fetch title for URL {url}: {e}")
        return None
    return title


def fetch_medium_titles(url):
    title = None
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        title_element = soup.find('title', {'data-rh': "true"})
        if title_element:
            title = title_element.get_text()
            logging.info(f"The title is: [{title}]")
        else:
            logging.info(f"Title not found for URL {url}")
    except Exception as e:
        logging.info(f"Could not fetch title for URL {url}: {e}")
        return None
    return title


def fetch_vitalik_ca_titles(url):
    title = None
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        title_element = soup.find('link', {'rel': 'alternate', 'type': 'application/rss+xml'})
        if title_element and 'title' in title_element.attrs:
            title = title_element['title'].strip()
            logging.info(f"Fetched title [{title}] for URL {url}")
    except Exception as e:
        logging.info(f"Could not fetch title for URL {url} using the rel='alternate' method: {e}")
    return title


def fetch_title(row, url_to_title):
    url = getattr(row, 'article')

    # Check if the title already exists in the output file
    if url in url_to_title and (url_to_title[url] is not None) and not pd.isna(url_to_title[url]):
        return url_to_title[url]

    # Define a mapping of URL patterns to functions
    url_patterns = {
        'ethresear.ch': fetch_discourse_titles,
        'collective.flashbots.net': fetch_discourse_titles,
        'lido.fi': fetch_discourse_titles,
        'research.anoma': fetch_discourse_titles,
        'frontier.tech': fetch_frontier_tech_titles,
        'vitalik.ca': fetch_vitalik_ca_titles,
        'writings.flashbots': fetch_flashbots_writings_titles,
        'medium.com': fetch_medium_titles,
        'blog.metrika': fetch_medium_titles,
        'mirror.xyz': fetch_mirror_titles,
        'iex.io': fetch_iex_titles,
        'paradigm.xyz': fetch_paradigm_titles,
        'hackmd.io': fetch_hackmd_titles,
        'jumpcrypto.com': fetch_jump_titles,
        'notion.site': fetch_notion_titles,  # Placeholder for fetch_notion_titles
        'notes.ethereum.org': fetch_notion_titles,  # Placeholder for fetch_notion_titles
        'succulent-throat-0ce.': fetch_notion_titles,  # Placeholder for fetch_notion_titles
        'propellerheads.xyz': fetch_propellerheads_titles,
        'a16z': fetch_a16z_titles,
        'blog.uniswap': None,  # Placeholder for fetch_uniswap_titles
        'osmosis.zone': fetch_osmosis_titles,
        'mechanism.org': fetch_mechanism_titles,
    }

    # TODO 2023-09-18: add substack support

    # Iterate through URL patterns and fetch titles
    for pattern, fetch_function in url_patterns.items():
        if pattern in url:
            if fetch_function:
                return fetch_function(url)
            else:
                return None

    return None  # Default case if no match is found


def fetch_article_titles(csv_filepaths, output_filepath):
    """
    Fetch the titles of articles from ethresear.ch present in the input CSV files and save them in a new CSV file.

    Parameters:
    - csv_filepaths (list): List of file paths of the input CSV files containing article URLs and referrers.
                            Each CSV file should have two columns with headers 'article' and 'referrer'.
    - output_filepath (str): The file path where the output CSV file with the fetched titles should be saved.

    Returns:
    - None
    """
    # Step 1: Read the CSV files with specified column names
    dfs = []
    for csv_filepath in csv_filepaths:
        df = pd.read_csv(csv_filepath)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    # Step 1.5: Try to read the existing output file to get already fetched titles
    try:
        output_df = pd.read_csv(output_filepath)
    except FileNotFoundError:
        output_df = pd.DataFrame(columns=['title', 'article', 'referrer'])

    url_to_title = dict(zip(output_df['article'], output_df['title']))

    # Step 2: Remove duplicates from the combined DataFrame
    combined_df.drop_duplicates(subset=['article'], inplace=True)

    # Step 3: Loop through the rows and fetch titles for specified URLs
    titles = []

    # Use ThreadPoolExecutor to fetch titles in parallel
    with ThreadPoolExecutor() as executor:
        titles = list(executor.map(fetch_title, combined_df.itertuples(), [url_to_title]*len(combined_df)))

    # Step 4: Save titles in a new column
    combined_df['title'] = titles

    # Step 5: Save the updated DataFrame to a new CSV file
    combined_df = combined_df[['title', 'article', 'referrer']]
    combined_df.to_csv(output_filepath, index=False)


# Usage example:
def run():
    csv_filepaths = [f'{root_directory()}/data/links/articles.csv', f'{root_directory()}/data/crawled_articles.csv']
    output_filepath = f'{root_directory()}/data/links/articles_updated.csv'
    fetch_article_titles(csv_filepaths, output_filepath)

if __name__ == '__main__':
    run()