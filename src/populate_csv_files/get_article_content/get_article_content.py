import datetime
import time
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
import requests
import pdfkit
import logging
import json
import os
import re
import markdown

from src.populate_csv_files.get_article_content.utils import safe_request, sanitize_mojibake, html_to_markdown, markdown_to_html, convert_date_format, convert_frontier_tech_date_format, convert_mirror_date_format, html_to_markdown_a16z
from src.populate_csv_files.get_article_content.get_flashbots_writings import fetch_flashbots_writing_contents_and_save_as_pdf
from src.utils import root_directory, return_driver
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


empty_content = {
        'content': None,
        'release_date': None,
        'authors': None,
        'author_urls': None,
        'author_firm_name': None,
        'author_firm_url': None
    }

def get_file_list(GITHUB_API_URL="https://api.github.com/repos/flashbots/flashbots-writings-website/contents/content"):
    response = requests.get(GITHUB_API_URL)
    if response.status_code == 200:
        return response.json()  # Returns a list of file information
    else:
        logging.info(f"Failed to fetch file list from {GITHUB_API_URL}, response code {response.status_code}")
        return []


def fetch_discourse_content_from_url(url, css_selector="div.post[itemprop='articleBody']"):
    """
    Fetch and neatly parse the content of an article from a URL using the specified CSS selector.
    The content is returned in Markdown format.

    Parameters:
    - url (str): The URL of the article.
    - css_selector (str): The CSS selector to locate the content container in the HTML.

    Returns:
    - str: The neatly parsed content of the article in Markdown format, or None if the content could not be fetched.
    """
    try:
        response = safe_request(url)  # Use the safe_request function to handle potential 429 errors.
        if response is None:
            return empty_content

        response.encoding = 'utf-8'  # Force UTF-8 encoding
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')
        content_container = soup.select_one(css_selector)

        if not content_container:
            logging.warning(f"No content found for URL {url} with selector {css_selector}")
            return empty_content

        markdown_content = ''.join(html_to_markdown(element) for element in content_container if element.name is not None)
        markdown_content = sanitize_mojibake(markdown_content)  # Clean up the content after parsing

        # Extracting release date
        meta_tag = soup.find('meta', {'property': 'article:published_time'})
        if meta_tag and 'content' in meta_tag.attrs:
            release_date = meta_tag['content'].split('T')[0]
        else:
            release_date = None

        # Find the 'a' tag within the 'span' with class 'creator'
        a_tag = soup.select_one('.creator a')

        # Extract the 'href' attribute
        authors = a_tag['href'] if a_tag else None

        logging.info(f"Fetched content for URL {url}")
        return {
            'content': markdown_content,
            'release_date': release_date,
            'authors': authors,
            'author_urls': None,
            'author_firm_name': None,
            'author_firm_url': None
        }
    except Exception as e:
        logging.error(f"Could not fetch content for URL {url}: {e}")
        return empty_content


def fetch_medium_content_from_url(url):
    try:
        response = requests.get(url)
        response.encoding = 'utf-8'
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')
        # Find the article tag
        article_tag = soup.find('article')
        # Within the article tag, find the section
        section_tag = article_tag.find('section') if article_tag else None

        # Within the section, find the third div which seems to be the container of the content
        content_div = section_tag.find_all('div')[2] if section_tag else None  # Indexing starts at 0; 2 means the third div

        # Initialize a list to hold all markdown content
        content_list = []

        # Loop through all content elements
        for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
            markdown = html_to_markdown(elem)  # Assuming html_to_markdown is defined
            content_list.append(markdown)

        # Join all the content into a single string with markdown
        markdown_content = '\n'.join(content_list)
        markdown_content = sanitize_mojibake(markdown_content)  # Assuming sanitize_mojibake is defined

        # Extract title
        title_tag = soup.find('title')
        title = title_tag.text if title_tag else None

        # Extract author URL
        author_meta_tag = soup.find('meta', {'property': 'article:author'})
        author_url = author_meta_tag['content'] if author_meta_tag else None

        author_name_tag = soup.find('meta', {'name': 'author'})
        author_name = author_name_tag['content'] if author_name_tag else None

        # Extract publish date using 'data-testid' attribute
        publish_date_tag = soup.find(attrs={"data-testid": "storyPublishDate"})
        publish_date = publish_date_tag.text.strip() if publish_date_tag else None
        publish_date = convert_date_format(publish_date)

        # Extract firm name and URL from script tag
        script_tag = soup.find('script', type='application/ld+json')
        if script_tag:
            data = json.loads(script_tag.string)
            author_firm_name = data.get("publisher", {}).get("name")
            author_firm_url = data.get("publisher", {}).get("url")
        else:
            author_firm_name = None
            author_firm_url = None

        return {
            'title': title,
            'content': markdown_content,
            'release_date': publish_date,
            'authors': author_name,
            'author_urls': author_url,
            'author_firm_name': author_firm_name,
            'author_firm_url': author_firm_url
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content

def fetch_mirror_content_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')

        # Extract title
        title_tag = soup.find('title')
        title = title_tag.text if title_tag else 'N/A'

        # Extract publish date
        publish_date_tag = soup.find('span', string=re.compile(r'\b(?:\d{1,2}(?:st|nd|rd|th), \d{4})\b'))
        publish_date = publish_date_tag.text.strip() if publish_date_tag else 'N/A'
        publish_date = convert_mirror_date_format(publish_date)  # Assuming convert_mirror_date_format is defined

        # Try to extract author using the new CSS selector
        author_name_div = soup.select_one(
            '#__next > div._1sjywpl0._1sjywpl1.bc5nciih.bc5ncit1.bc5nci37p > div > div._1sjywpl0._1sjywpl1.bc5nci23u.bc5nci2bm.bc5nci3mz.bc5nci3na.bc5nci3ti.bc5nci3tt.bc5nci316.bc5nci4ow > div > div._1sjywpl0._1sjywpl1.bc5nci5.bc5nciih.bc5nciuz.bc5nciyg.bc5nci1hr > a > div > div > div._1sjywpl0._1sjywpl1.bc5nci546.bc5nci4sx.bc5nciwd.bc5ncix7._2gklefg')
        author_name = author_name_div.text if author_name_div else None

        # Extract author URL
        author_a_tag = soup.find('a', {'data-state': 'closed'})

        # Extract the URL
        author_url = author_a_tag['href'] if author_a_tag and author_a_tag.has_attr('href') else None

        # If the new method didn't work, fall back to the old method
        if not author_name:
            # Extract author name
            # Find the div that contains the author name 'Rated'
            author_name_div = soup.find('div', class_='_2gklefg')
            author_name = author_name_div.text if author_name_div else 'N/A'

            if (author_name == 'N/A') or author_name == '':
                author_details = extract_mirror_author_details(url)
                author_name = author_details['authors']

        # Initialize a list to hold all markdown content
        content_list = []

        # Find the container for the content
        content_container = soup.select_one('.css-72ne1l > div:nth-child(1)')
        if content_container is not None:
            # Loop through all content elements within the container
            for elem in content_container.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'div']):
                markdown = html_to_markdown(elem)  # Assuming html_to_markdown is defined
                content_list.append(markdown)

        # Join all the content into a single string with markdown
        markdown_content = '\n'.join(content_list)
        markdown_content = sanitize_mojibake(markdown_content)  # Assuming sanitize_mojibake is defined

        return {
            'title': title,
            'content': markdown_content,
            'release_date': publish_date,
            'authors': author_name,
            'author_urls': author_url,
            'author_firm_name': None,  # No information provided for this
            'author_firm_url': None  # No information provided for this
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_frontier_tech_content_from_url(url):
    try:
        response = requests.get(url)
        response.encoding = 'utf-8'
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')

        # Extract title
        title_tag = soup.find('h1', class_='notion-header__title')
        title = title_tag.text.strip() if title_tag else None

        # Extract author
        author_tag = soup.find('div', class_='notion-callout__content')
        author = author_tag.text.strip() if author_tag else None

        # Extract date
        date_tag = soup.find('div', class_='notion-callout__content', string=re.compile(r'\d{1,2} \w+ \d{4}'))
        date = date_tag.text.strip() if date_tag else None
        date = convert_frontier_tech_date_format(date)

        # Extract content
        content_list = []
        for content_tag in soup.select('.notion-text, .notion-heading, .notion-bulleted-list'):
            markdown = html_to_markdown(content_tag)
            content_list.append(markdown)
        content_markdown = ''.join(content_list)

        return {
            'title': title,
            'authors': author,
            'release_date': date,
            'content': content_markdown
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def extract_notion_content(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    content_list = []

    # This function will extract text and links recursively and maintain structure
    def extract_content(html_element):
        for child in html_element.children:
            if child.name == 'a':
                href = child.get('href', '')
                text = child.get_text(strip=True)
                if href:
                    content_list.append(f'[{text}]({href})')
            elif child.name in {'div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'br'} and not isinstance(child, str):
                # Add a newline before the block-level element if the last addition wasn't a newline
                if content_list and content_list[-1] != '\n':
                    content_list.append('\n')
                extract_content(child)
                # Add a newline after the block-level element
                if child.name != 'div':
                    content_list.append('\n')
            elif child.name is None and isinstance(child, str):  # This is a NavigableString
                stripped_text = child.strip()
                if stripped_text:
                    # Append text to content list, maintaining inline structure
                    content_list.append(stripped_text + ' ')
            else:
                # If it's none of the above, recurse into the child
                extract_content(child)

    # Find the main content container
    content_div = soup.find('div', class_='notion-page-content')
    if content_div:
        extract_content(content_div)

    # Correctly join the content list while respecting the structure
    structured_content = ''.join(content_list).strip()
    # Replace multiple newlines with a single newline
    structured_content = '\n\n'.join(filter(None, structured_content.split('\n')))

    return structured_content


def extract_author_details(url):
    """
    Extracts the author name and author URL from a given URL.

    :param url: The URL to extract details from.
    :return: A dictionary with author name and author URL.
    """
    parsed_url = urlparse(url)
    subdomain = parsed_url.netloc.split('.')[0]  # Assuming the author name is the subdomain
    domain = parsed_url.netloc

    if subdomain == 'www':
        # If the subdomain is www, we assume the second part is the author name
        author_name = parsed_url.netloc.split('.')[1]
    else:
        author_name = subdomain

    author_url = f"{parsed_url.scheme}://{domain}"

    return {
        'authors': author_name,
        'authors_urls': author_url
    }


def extract_hackmd_author_details(url):
    """
    Extracts the author name and author URL from a given HackMD URL.

    :param url: The HackMD URL to extract details from.
    :return: A dictionary with author name, author URL, and a flag indicating if it's a website.
    """
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split('/')

    # Check if there is an author part in the path
    if len(path_parts) > 1 and path_parts[1].startswith('@'):
        author_name = path_parts[1][1:]  # Remove '@' to get the author name
        author_url = f"https://hackmd.io/@{author_name}"
        # Determine if it's a website or an article
        is_website = len(path_parts) == 2 or (len(path_parts) == 3 and path_parts[2] == '')
        return {
            'authors': author_name,
            'authors_urls': author_url,
            'is_website': is_website
        }
    else:
        # No author in the URL, return None
        return {
            'authors': None,
            'authors_urls': None,
            'is_website': False
        }


def extract_mirror_author_details(url):
    """
    Extracts the author name from a given mirror.xyz URL.

    :param url: The mirror.xyz URL to extract details from.
    :return: A dictionary with author name and a flag indicating if it's a website.
    """
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    path_parts = parsed_url.path.split('/')

    # Default values
    author_name = None
    is_website = False

    # Check if there is a subdomain (author name as subdomain)
    domain_parts = domain.split('.')
    if domain_parts[0] != 'mirror' and len(domain_parts) > 2:
        author_name = domain_parts[0]
        is_website = True  # A subdomain usually indicates a personal site
    # Check if the author name is in the path
    elif len(path_parts) > 1 and path_parts[1].endswith('.eth'):
        author_name = path_parts[1]
        is_website = False  # No subdomain usually indicates an article

    # Construct the author URL if author_name was found
    author_url = f"https://{domain}/{author_name}" if author_name else None

    return {
        'authors': author_name,
        'authors_urls': author_url,
        'is_website': is_website
    }


def fetch_notion_content_from_url(url):
    try:
        driver = return_driver()

        try:
            driver.get(url)

            # Wait for some time to allow JavaScript to load content
            driver.implicitly_wait(10)  # Adjust the wait time as necessary
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Get the page title
            title = driver.title

            date_pattern = re.compile(r'\d{4}/\d{2}/\d{2}')
            # Find the release date using the date pattern
            date_match = date_pattern.search(soup.get_text())
            publish_date = date_match.group(0) if date_match else 'N/A'

            page_source = driver.page_source  # The page source obtained from Selenium
            content = extract_notion_content(page_source)

            author_details = extract_author_details(url)
            print(f"Fetched title [{title}] for URL {url}")

            return {
                'title': title,
                'content': content,
                'release_date': publish_date,
                'authors': author_details['authors'],  # get author name which is the URL subdomain
                'author_urls': author_details['authors_urls'],  # get the whole URL domain
                'author_firm_name': None,  # No information provided for this
                'author_firm_url': None  # No information provided for this
            }
        except Exception as e:
            print(f"Error fetching content from URL {url}: {e}")
            return empty_content
    finally:
        # Always close the browser to clean up
        driver.quit()


def extract_hackmd_content(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    content_list = []

    # This function will extract text and links recursively and maintain structure
    def extract_content(html_element):
        for child in html_element.children:
            if child.name == 'a':
                href = child.get('href', '')
                text = child.get_text(strip=True)
                if href:
                    content_list.append(f'[{text}]({href})')
            elif child.name in {'div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'br'} and not isinstance(child, str):
                # Add a newline before the block-level element if the last addition wasn't a newline
                if content_list and content_list[-1] != '\n':
                    content_list.append('\n')
                extract_content(child)
                # Add a newline after the block-level element
                if child.name != 'div':
                    content_list.append('\n')
            elif child.name is None and isinstance(child, str):  # This is a NavigableString
                stripped_text = child.strip()
                if stripped_text:
                    # Append text to content list, maintaining inline structure
                    content_list.append(stripped_text + ' ')
            else:
                # If it's none of the above, recurse into the child
                extract_content(child)

    # Find the main content container
    content_div = soup.find('div', id='doc')
    if content_div:
        extract_content(content_div)

    # Correctly join the content list while respecting the structure
    structured_content = ''.join(content_list).strip()
    # Replace multiple newlines with a single newline
    structured_content = '\n\n'.join(filter(None, structured_content.split('\n')))

    return structured_content


def fetch_hackmd_article_content(url):
    """
    Fetch the title of an article from a HackMD URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - str: The title of the article, or None if the title could not be fetched.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        author_details = extract_hackmd_author_details(url)
        if author_details['is_website']:
            return empty_content

        title_tag = soup.find('title')
        title = title_tag.text if title_tag else 'N/A'

        release_date = soup.select_one('.ui-status-lastchange').parent.contents[3].get('data-createtime')
        release_date = datetime.datetime.fromtimestamp(int(release_date)/1000).strftime('%Y-%m-%d') if release_date is not None else 'N/A'
        # Find the main content container
        content = extract_hackmd_content(response.content)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': author_details['authors'],
            'author_urls': author_details['authors_urls'],
            'author_firm_name': None,  # No information provided for this
            'author_firm_url': None  # No information provided for this
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_vitalik_ca_article_content(url):
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


def fetch_paradigm_article_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the title
        title_tag = soup.select_one('.Post_post__J7vh4 h1')
        title = title_tag.text.strip() if title_tag else 'N/A'

        # Extract the publish date
        details_tag = soup.select_one('.Post_post__details__W3e0e')
        release_date = None
        if details_tag:
            # Find date within the details tag
            date_str = details_tag.text.strip().split('|')[0].strip()
            release_date = datetime.datetime.strptime(date_str, '%b %d, %Y').strftime('%Y-%m-%d')

        # Extract the authors
        authors = []
        author_urls = []
        for author_tag in details_tag.select('span a'):
            authors.append(author_tag.text.strip())
            if 'team' in author_tag["href"]:
                author_url = f'https://www.paradigm.xyz{author_tag["href"]}'
            else:
                author_url = author_tag["href"]
            author_urls.append(author_url)

        # Extract the content
        content_div = soup.select_one('.Post_post__content__dMuW4')
        if content_div is None:
            content_div = soup.select_one('.Post_post__content__dmuW4')
        content_list = []
        if content_div is not None:
            # Loop through all content elements within the container
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'div']):
                markdown = html_to_markdown(elem)  # Assuming html_to_markdown is defined
                content_list.append(markdown)

        # Join all the content into a single string with markdown
        content = '\n'.join(content_list)
        return {
            'title': title,
            'content': content.strip(),
            'release_date': release_date,
            'authors': authors,
            'author_urls': ['https://mirror.xyz' + url for url in author_urls],
            'author_firm_name': None,  # No information provided for this
            'author_firm_url': None  # No information provided for this
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_propellerheads_article_content(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the title
        title_tag = soup.select_one('.article-title_heading')
        title = title_tag.text.strip() if title_tag else 'N/A'

        # Extract the publish date
        date_tag = soup.select_one('.article-title_content > div:nth-child(1)')
        release_date = None
        if date_tag:
            date_str = date_tag.text.strip()
            # Assuming the date format is like "August 7, 2023"
            release_date = datetime.datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')

        # Extract the author name
        author_tag = soup.select_one('.article-content_author-text > div:nth-child(2)')
        authors = [author_tag.text.strip()] if author_tag else []

        # Extract the content
        content_div = soup.select_one('.article-content_rich-text')
        content_list = []
        if content_div:
            # Loop through all content elements within the container
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                markdown = html_to_markdown(elem)  # Assuming html_to_markdown is defined
                content_list.append(markdown)

        # Join all the content into a single string with newlines
        content = '\n\n'.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': authors,
            'author_urls': [],  # Assuming no specific author URL is provided
            'author_firm_name': None,
            'author_firm_url': None
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_jump_article_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the title
        title_tag = soup.select_one('h1.MuiTypography-root')
        title = title_tag.text.strip() if title_tag else 'N/A'

        # Extract the author name
        author_tag = soup.select_one('h6.MuiTypography-root:nth-child(2)')
        authors = [author_tag.text.strip()] if author_tag else []

        # Extract the publish date
        date_tag = soup.select_one('.css-k1xgly')
        release_date = None
        if date_tag:
            date_str = date_tag.text.strip().split('_')[0].strip()
            # Assuming the date format is like "Apr 20 2022"
            release_date = datetime.datetime.strptime(date_str, '%b %d %Y').strftime('%Y-%m-%d')

        # Extract the content
        content_div = soup.select_one('#postStyle')
        content_list = []
        if content_div:
            # Loop through all content elements within the container
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                markdown = html_to_markdown(elem)  # Assuming html_to_markdown is defined
                content_list.append(markdown)

        # Join all the content into a single string with newlines
        content = ''.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': authors,
            'author_urls': [],  # Assuming no specific author URL is provided
            'author_firm_name': None,
            'author_firm_url': None
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_a16z_article_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the title
        title_tag = soup.select_one('.highlight-display > h2:nth-child(1)')
        title = title_tag.text.strip() if title_tag else 'N/A'

        # Extract the authors
        authors = []
        author_tags = soup.select('.sep-comma-and span')
        for tag in author_tags:
            authors.append(tag.text.strip())

        # Extract the publish date
        date_tag = soup.select_one('.caption-2')
        release_date = None
        if date_tag:
            date_str = date_tag.text.strip()
            # Assuming the date format is like "8.24.23"
            date_components = date_str.split('.')
            if len(date_components) == 3:
                month, day, year_suffix = date_components
                current_year = datetime.datetime.now().year
                year = str(current_year)[:2] + year_suffix
                formatted_date_str = f"{month}.{day}.{year}"
                release_date = datetime.datetime.strptime(formatted_date_str, '%m.%d.%Y').strftime('%Y-%m-%d')
            else:
                release_date = 'N/A'

        # Extract the content
        content_div = soup.select_one('.wysiwyg')
        content_list = []
        if content_div:
            # Loop through all content elements within the container
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                markdown = html_to_markdown_a16z(elem)  # Assuming html_to_markdown is defined
                content_list.append(markdown)

        # Join all the content into a single string with newlines
        content = ''.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': authors,
            'author_urls': [],  # Assuming no specific author URL is provided
            'author_firm_name': None,
            'author_firm_url': None
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_dba_article_content(url):
    """
    Fetch the content of an article from a dba.xyz URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - dict: A dictionary with title, content, release date, authors, and author URLs of the article.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_selector = '.page-section > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > h1:nth-child(2)'
        title_tag = soup.select_one(title_selector)
        title = title_tag.get_text(strip=True) if title_tag else 'N/A'

        # Extract release date
        release_date_selector = '.post-meta-data > span:nth-child(1)'
        release_date_tag = soup.select_one(release_date_selector)
        if release_date_tag:
            # Get the current date
            current_date = datetime.datetime.now()
            # Extract the text for the date
            date_text = release_date_tag.get_text(strip=True)
            # Parse the date without the year
            extracted_date = datetime.datetime.strptime(date_text, '%B %d')
            # Assign the current year initially
            year = current_date.year
            # Check if the extracted date is later in the year than the current date
            if extracted_date.month > current_date.month or \
                    (extracted_date.month == current_date.month and extracted_date.day > current_date.day):
                # If the extracted date is later in the year, it must be from the previous year
                year -= 1
            # Combine the extracted date with the year
            release_date = datetime.datetime(year, extracted_date.month, extracted_date.day).strftime('%Y-%m-%d')
        else:
            release_date = 'N/A'

        # Extract author
        author_selector = '.post-meta-data > a:nth-child(2)'
        author_tag = soup.select_one(author_selector)
        author_name = author_tag.get_text(strip=True) if author_tag else 'N/A'
        author_url = author_tag['href'] if author_tag and author_tag.has_attr('href') else 'N/A'

        # Extract content
        content_selector = '.page-section > div:nth-child(1) > div:nth-child(1) > div:nth-child(1)'
        content_div = soup.select_one(content_selector)
        content_list = []
        if content_div:
            # Loop through all content elements within the container
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                # Text from each element is extracted and added to the content list
                content_list.append(html_to_markdown(elem))  # Assuming html_to_markdown is defined

        # Join all the content into a single string with newlines
        content = '\n\n'.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': author_name,
            'author_urls': author_url,
            'author_firm_name': None,  # No information provided for this
            'author_firm_url': None  # No information provided for this
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content

def fetch_iex_article_content(url):
    """
    Fetch the content of an article from a iex.io URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - dict: A dictionary with title, content, release date, authors, and author URLs of the article.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_tag = soup.select_one('.header-content-title')
        title = title_tag.get_text(strip=True) if title_tag else 'N/A'

        # Extract release date
        release_date_tag = soup.select_one('div.se-date:nth-child(3)')
        if release_date_tag:
            date_str = release_date_tag.get_text(strip=True)
            release_date = datetime.datetime.strptime(date_str, '%b %d, %Y').strftime('%Y-%m-%d')
        else:
            release_date = 'N/A'

        # Extract author and corporate title
        author_name_tag = soup.select_one('div.label:nth-child(2)')
        author_name = author_name_tag.get_text(strip=True) if author_name_tag else 'N/A'
        corporate_title_tag = soup.select_one('.paragraph-large')
        corporate_title = corporate_title_tag.get_text(strip=True) if corporate_title_tag else 'N/A'
        authors = f"{author_name} - {corporate_title}" if author_name != 'N/A' else 'N/A'

        # Extract content
        content_div = soup.select_one('.summary-component-content')
        content_list = []
        if content_div:
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                content_list.append(html_to_markdown(elem))  # Convert HTML to Markdown

        content = '\n\n'.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': authors,
            'author_urls': '',  # URL not provided in the new format
            'author_firm_name': '',  # Not provided
            'author_firm_url': ''  # Not provided
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content



def fetch_uniswap_article_content(url):
    """
    Fetch the content of an article from a uniswap blog URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - dict: A dictionary with title, content, release date, and authors of the article.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_tag = soup.select_one('head > title:nth-child(4)')
        title = title_tag.get_text(strip=True).split('|')[0].strip() if title_tag else 'N/A'

        # Extract release date
        release_date_tag = soup.select_one('div.Type__SubTitle-sc-ga2v53-3:nth-child(2)')
        if release_date_tag:
            date_str = release_date_tag.get_text(strip=True)
            release_date = datetime.datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')
        else:
            release_date = 'N/A'

        # Authors are not specified on the page, set as 'N/A'
        authors = 'N/A'

        # Extract content
        content_div = soup.select_one('.slug__ProseWrapper-sc-1flmkfl-4')
        content_list = []
        if content_div:
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'blockquote']):
                content_list.append(html_to_markdown(elem))  # Convert HTML to Markdown

        content = '\n\n'.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': authors
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_substack_article_content(url):
    """
    Fetch the content of an article from a Substack URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - dict: A dictionary with title, content, release date, and authors of the article.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_tag = soup.select_one('.post-title')
        title = title_tag.get_text(strip=True) if title_tag else 'N/A'

        # Extract author
        author_tag = soup.select_one('a.frontend-pencraft-Text-module__decoration-hover-underline--BEYAn')
        author_name = author_tag.get_text(strip=True) if author_tag else 'N/A'

        # Extract release date
        date_tag = soup.select_one('div.frontend-pencraft-Text-module__color-pub-secondary-text--OzRTa:nth-child(1)')
        date_str = date_tag.get_text(strip=True) if date_tag else 'N/A'
        try:
            release_date = datetime.datetime.strptime(date_str, '%b %d, %Y').strftime('%Y-%m-%d')
        except ValueError:
            # Handle different date format if necessary
            release_date = 'N/A'

        # Extract content
        content_div = soup.select_one('.body')
        content_list = []
        if content_div:
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                content_list.append(html_to_markdown(elem))  # Convert HTML to Markdown

        content = '\n\n'.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': author_name
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return {
            'title': '',
            'content': '',
            'release_date': '',
            'authors': ''
        }


def fetch_vitalik_article_content(url):
    """
    Fetch the content of an article from a Vitalik Buterin's website URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - dict: A dictionary with title, content, release date, and author of the article.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title from the meta tag
        title_tag = soup.find('meta', attrs={'name': 'twitter:title'})
        title = title_tag['content'] if title_tag else 'N/A'

        # Author is always 'Vitalik Buterin'
        author_name = 'Vitalik Buterin'

        # Extract release date from the URL
        date_match = re.search(r'/(\d{4}/\d{2}/\d{2})/', url)
        release_date = date_match.group(1).replace('/', '-') if date_match else 'N/A'

        # Extract content
        content_div = soup.select_one('#doc')
        content_list = []
        if content_div:
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                # Assuming html_to_markdown is a function you've defined elsewhere to convert HTML to Markdown
                content_list.append(html_to_markdown(elem))  # Convert HTML to Markdown

        content = '\n\n'.join(content_list)

        return {
            'title': title,
            'content': content,
            'release_date': release_date,
            'authors': author_name
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content

def fetch_monoceros_article_content(url):
    """
    Fetch the content of an article from a Monoceros URL.

    Parameters:
    - url (str): The URL of the article.

    Returns:
    - dict: A dictionary with title, content, and authors of the article.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_tag = soup.select_one('h1.rY14H8')
        title = title_tag.get_text(strip=True) if title_tag else 'N/A'

        # Extract authors
        authors_tag = soup.select_one('section.n_FyzB:nth-child(1) > div:nth-child(2) > div:nth-child(2)')
        authors = authors_tag.get_text(strip=True) if authors_tag else 'N/A'

        # Extract content
        content_div = soup.select_one('section.n_FyzB:nth-child(1) > div:nth-child(2) > div:nth-child(4)')
        content_list = []
        if content_div:
            for elem in content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol']):
                # Assuming html_to_markdown is a function you've defined elsewhere to convert HTML to Markdown
                content_list.append(html_to_markdown(elem))  # Convert HTML to Markdown

        content = ''.join(content_list)
        return {
            'title': title,
            'content': content,
            'release_date': 'N/A',  # No release date available
            'authors': authors
        }
    except Exception as e:
        print(f"Error fetching content from URL {url}: {e}")
        return empty_content


def fetch_content(row, output_dir):
    url = getattr(row, 'article')

    # Define a mapping of URL patterns to functions

    url_patterns = {
        'ethresear.ch': fetch_discourse_content_from_url,
        'collective.flashbots.net': fetch_discourse_content_from_url,
        'lido.fi': fetch_discourse_content_from_url,
        'research.anoma': fetch_discourse_content_from_url,
        'gov.uniswap.org': fetch_discourse_content_from_url,
        'governance.aave.com': fetch_discourse_content_from_url,
        'forum.celestia.org': fetch_discourse_content_from_url,
        'research.arbitrum.io': fetch_discourse_content_from_url,
        'dydx.forum': fetch_discourse_content_from_url,
        'forum.arbitrum.foundation': fetch_discourse_content_from_url,
        'frontier.tech': fetch_frontier_tech_content_from_url,
        # 'vitalik.ca': fetch_vitalik_ca_article_content,  # TODO 2023-12-23
        'medium.com': fetch_medium_content_from_url,
        'blog.metrika': fetch_medium_content_from_url,
        'mirror.xyz': fetch_mirror_content_from_url,
        'iex.io': fetch_iex_article_content,
        'paradigm.xyz': fetch_paradigm_article_content,
        'hackmd.io': fetch_hackmd_article_content,
        'jumpcrypto.com': fetch_jump_article_content,
        'notion.site': fetch_notion_content_from_url,
        'notes.ethereum.org': fetch_hackmd_article_content,
        'dba.xyz':  fetch_dba_article_content,
        'propellerheads.xyz': fetch_propellerheads_article_content,
        'a16z': fetch_a16z_article_content,
        'blog.uniswap': fetch_uniswap_article_content,
        'substack.com': fetch_substack_article_content,
        'vitalik.eth.limo': fetch_vitalik_article_content,
        # 'osmosis.zone': fetch_osmosis_article_content,
        'monoceros': fetch_monoceros_article_content,
    }

    for pattern, fetch_function in url_patterns.items():
        if pattern in url:
            if fetch_function:
                content_info = fetch_function(url)
                return content_info

    # Default case if no match is found
    return empty_content


def update_csv(full_df, df, modified_indices, csv_filepath):
    """
    Update specific rows in the full DataFrame based on the modified subset DataFrame and save it to a CSV file.

    :param full_df: The original full DataFrame.
    :param df: The modified subset DataFrame.
    :param modified_indices: A list of indices in df corresponding to the rows modified.
    :param csv_filepath: File path of the CSV file to be updated.
    """
    for idx in modified_indices:
        # Identify the article URL in the modified subset DataFrame
        article_url = df.iloc[idx]['article']

        # Find the corresponding row in the full DataFrame based on article URL
        full_row_index = full_df[full_df['article'] == article_url].index

        # Update the corresponding row in full_df with the modified row in df
        if not full_row_index.empty:
            full_df.loc[full_row_index[0]] = df.iloc[idx]

    # Write the updated full DataFrame to CSV
    full_df.to_csv(csv_filepath, index=False)


def fetch_article_contents_and_save_as_pdf(csv_filepath, output_dir, num_articles=None, overwrite=True, url_filters=None, thread_count=None):
    """
    Fetch the contents of articles and save each content as a PDF in the specified directory.

    Parameters:
    - csv_filepath (str): The file path of the input CSV file containing article URLs and referrers.
    - output_dir (str): The directory where the article PDFs should be saved.
    - num_articles (int, optional): Number of articles to process. If None, process all articles.
    """
    # Read the entire CSV file into a DataFrame
    full_df = pd.read_csv(csv_filepath)
    if 'release_date' not in full_df.columns:
        full_df['release_date'] = None
    if 'authors' not in full_df.columns:
        full_df['authors'] = None

    # Create a filtered subset for processing
    df = full_df.copy()
    if url_filters is not None:
        df = df[df['article'].str.contains('|'.join(url_filters))]
    if num_articles is not None:
        df = df.head(num_articles)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # List to store indices of modified rows
    modified_indices = []

    # Function to process each row
    def process_row(row, index):
        nonlocal modified_indices
        article_url = getattr(row, 'article')
        article_title = getattr(row, 'title')

        import numpy as np
        if article_title is np.nan:
            article_title = article_url

        # Create a sanitized file name for the PDF from the article title
        pdf_filename = os.path.join(output_dir, article_title.replace("/", "<slash>") + '.pdf')

        # Check if PDF already exists
        if not os.path.exists(pdf_filename) or overwrite:
            content_info = fetch_content(row, output_dir)
            if content_info['content']:
                # Specify additional options for pdfkit to ensure UTF-8 encoding
                options = {
                    'encoding': "UTF-8",
                    'custom-header': [
                        ('Content-Encoding', 'utf-8'),
                    ],
                    'no-outline': None
                }
                if article_title == article_url:
                    # Create a sanitized file name for the PDF from the article title
                    if content_info['title']:
                        article_title = content_info['title']
                        df.loc[df['article'] == getattr(row, 'article'), 'title'] = article_title
                        pdf_filename = os.path.join(output_dir, article_title.replace("/", "<slash>") + '.pdf')

                pdfkit.from_string(markdown_to_html(content_info['content']), pdf_filename, options=options)
                logging.info(f"Saved PDF [{article_title}] for {article_url}")

            else:
                # logging.warning(f"No content fetched for {article_url}")
                pass
                # Update the dataframe with the release date
            if content_info['release_date']:
                df.loc[df['title'] == getattr(row, 'title'), 'release_date'] = content_info['release_date']
            if content_info['authors']:
                df.loc[df['title'] == getattr(row, 'title'), 'authors'] = content_info['authors']

        # Update the modified rows list
        modified_indices.append(index)

    # Use ThreadPoolExecutor to process rows in parallel
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        executor.map(lambda pair: process_row(pair[1], pair[0]), enumerate(df.itertuples()))

    # Update only the modified rows in the full DataFrame
    update_csv(full_df, df, modified_indices, csv_filepath)


def add_new_articles():
    # Paths to the CSV files
    original_csv_file_path = os.path.join(root_directory(), 'data', 'links', 'articles.csv')
    updated_csv_file_path = os.path.join(root_directory(), 'data', 'links', 'articles_updated.csv')

    # Read the original and updated articles into DataFrames
    original_df = pd.read_csv(original_csv_file_path)
    updated_df = pd.read_csv(updated_csv_file_path)

    # Assuming 'article' is the column name that uniquely identifies rows
    # Identify new articles not present in the updated DataFrame
    new_articles = original_df[~original_df['article'].isin(updated_df['article'])]

    # Append new unique articles to the updated DataFrame and save
    if not new_articles.empty:
        # Use concat instead of append and drop duplicates just in case
        updated_df = pd.concat([updated_df, new_articles]).drop_duplicates(subset='article')
        # Save the updated DataFrame back to the updated CSV file
        updated_df.to_csv(updated_csv_file_path, index=False)


def run(url_filters=None, get_flashbots_writings=True, thread_count=None, overwrite=False):
    csv_file_path = f'{root_directory()}/data/links/articles_updated.csv'
    output_directory = f'{root_directory()}/data/articles_pdf_download/'
    add_new_articles()
    fetch_article_contents_and_save_as_pdf(csv_filepath=csv_file_path,
                                           output_dir=output_directory,
                                           overwrite=overwrite,
                                           url_filters=url_filters,
                                           thread_count=thread_count)
    if get_flashbots_writings:
        fetch_flashbots_writing_contents_and_save_as_pdf(output_directory)


if __name__ == "__main__":
    url_filters = ['gov.uniswap.org', 'governance.aave']  # ['a16z']  # ['pbs']  # None # ['hackmd']
    thread_count = 20
    get_flashbots_writings = False
    run(url_filters=url_filters, get_flashbots_writings=get_flashbots_writings, thread_count=thread_count, overwrite=True)
