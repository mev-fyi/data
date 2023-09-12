import time
import random

import pandas as pd
from bs4 import BeautifulSoup
import requests
from src.utils import root_directory
from concurrent.futures import ThreadPoolExecutor


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
        print(f"Fetched title [{title}] for URL {url}")
        return title
    except Exception as e:
        print(f"Could not fetch title for URL {url}: {e}")
        return None


def fetch_discourse_titles(url):
    return fetch_title_from_url(url, 'title')


def fetch_frontier_tech_titles(url):
    return fetch_title_from_url(url, 'head > title:nth-child(4)')


def fetch_flashbots_writings_titles(url):
    return fetch_title_from_url(url, '.blogPostTitle_RC3s')


def fetch_medium_titles(url):
    title = None
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        title_element = soup.find('title', {'data-rh': "true"})
        if title_element:
            title = title_element.get_text()
            print(f"The title is: [{title}]")
        else:
            print(f"Title not found for URL {url}")
    except Exception as e:
        print(f"Could not fetch title for URL {url}: {e}")
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
            print(f"Fetched title [{title}] for URL {url}")
    except Exception as e:
        print(f"Could not fetch title for URL {url} using the rel='alternate' method: {e}")
    return title


def fetch_title(row, url_to_title):
    url = getattr(row, 'article')

    # Check if the title already exists in the output file
    if url in url_to_title and (url_to_title[url] is not None) and not pd.isna(url_to_title[url]):
        return url_to_title[url]

    # do a random sleep from 1 to 3 seconds
    time.sleep(random.randint(1, 3))

    if 'ethresear.ch' in url or 'collective.flashbots.net' in url:
        return fetch_discourse_titles(url)
    elif 'frontier.tech' in url:
        return fetch_frontier_tech_titles(url)
    elif 'vitalik.ca' in url:
        return fetch_vitalik_ca_titles(url)
    elif 'writings.flashbots' in url:
        return fetch_flashbots_writings_titles(url)
    elif 'medium.com' in url:
        return fetch_medium_titles(url)
    else:
        return None


def fetch_article_titles(csv_filepath, output_filepath):
    """
    Fetch the titles of articles from ethresear.ch present in the input CSV file and save them in a new CSV file.

    Parameters:
    - csv_filepath (str): The file path of the input CSV file containing article URLs and referrers.
                          The CSV file should have two columns with headers 'article' and 'referrer'.
    - output_filepath (str): The file path where the output CSV file with the fetched titles should be saved.

    Returns:
    - None
    """
    # Step 1: Read the CSV file with specified column names
    df = pd.read_csv(csv_filepath)

    # Step 1.5: Try to read the existing output file to get already fetched titles
    try:
        output_df = pd.read_csv(output_filepath)
    except FileNotFoundError:
        output_df = pd.DataFrame(columns=['title', 'article', 'referrer'])

    url_to_title = dict(zip(output_df['article'], output_df['title']))

    # Step 2: Loop through the rows and fetch titles for specified URLs
    titles = []

    # Use ThreadPoolExecutor to fetch titles in parallel
    with ThreadPoolExecutor() as executor:
        titles = list(executor.map(fetch_title, df.itertuples(), [url_to_title]*len(df)))

    # Step 3: Save titles in a new column
    df['title'] = titles

    # Step 4: Save the updated DataFrame to a new CSV file
    df = df[['title', 'article', 'referrer']]
    df.to_csv(output_filepath, index=False)


# Usage example:
fetch_article_titles(f'{root_directory()}/data/links/articles.csv', f'{root_directory()}/data/links/articles_updated.csv')