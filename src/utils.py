import asyncio
import concurrent.futures
import csv
import json
import logging
import os
import subprocess
import time
from pathlib import Path
from random import choice
from typing import Optional, List, Union

import PyPDF2
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from requests import Session
from selenium import webdriver

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
    # Add more user agents if desired...
]
MAX_RETRIES = 3
MAX_DELAY = 30

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def root_directory() -> str:
    """
    Determine the root directory of the project. It checks if it's running in a Docker container and adjusts accordingly.

    Returns:
    - str: The path to the root directory of the project.
    """

    # Check if running in a Docker container
    if os.path.exists('/.dockerenv'):
        # If inside a Docker container, use '/app' as the root directory
        return '/app'

    # If not in a Docker container, try to use the git command to find the root directory
    try:
        git_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], stderr=subprocess.STDOUT)
        return git_root.strip().decode('utf-8')
    except subprocess.CalledProcessError:
        # Git command failed, which might mean we're not in a Git repository
        # Fall back to manual traversal
        pass
    except Exception as e:
        # Some other error occurred while trying to execute git command
        print(f"An error occurred while trying to find the Git repository root: {e}")

    # Manual traversal if git command fails
    current_dir = os.getcwd()
    root = os.path.abspath(os.sep)
    traversal_count = 0  # Track the number of levels traversed

    while current_dir != root:
        try:
            if 'src' in os.listdir(current_dir):
                print(f"Found root directory: {current_dir}")
                return current_dir
            current_dir = os.path.dirname(current_dir)
            traversal_count += 1
            print(f"Traversal count # {traversal_count}")
            if traversal_count > 10:
                raise Exception("Exceeded maximum traversal depth (more than 10 levels).")
        except PermissionError as e:
            # Could not access a directory due to permission issues
            raise Exception(f"Permission denied when accessing directory: {current_dir}") from e
        except FileNotFoundError as e:
            # The directory was not found, which should not happen unless the filesystem is changing
            raise Exception(f"The directory was not found: {current_dir}") from e
        except OSError as e:
            # Handle any other OS-related errors
            raise Exception("An OS error occurred while searching for the Git repository root.") from e

    # If we've reached this point, it means we've hit the root of the file system without finding a .git directory
    raise Exception("Could not find the root directory of the project. Please make sure you are running this script from within a Git repository.")


def ensure_newline_in_csv(csv_file: str) -> None:
    """
    Ensure that a CSV file ends with a newline.

    Parameters:
    - csv_file (str): Path to the CSV file.
    """
    try:
        with open(csv_file, 'a+', newline='') as f:  # Using 'a+' mode to allow reading
            # Move to the beginning of the file to check its content
            f.seek(0, os.SEEK_SET)
            if not f.read():  # File is empty
                return

            # Move to the end of the file
            f.seek(0, os.SEEK_END)

            # Check if the file ends with a newline, if not, add one
            if f.tell() > 0:
                f.seek(f.tell() - 1, os.SEEK_SET)
                if f.read(1) != '\n':
                    f.write('\n')
    except Exception as e:
        logging.error(f"Failed to ensure newline in {csv_file}. Error: {e}")


def read_existing_papers(csv_file):
    """
    Read existing paper titles from a CSV file.

    Parameters:
    - csv_file (str): Path to the CSV file where details are saved.

    Returns:
    - list: A list of titles of existing papers.
    """
    if not os.path.exists(csv_file):
        return []

    with open(csv_file, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row['title'] for row in reader]


def paper_exists_in_csv(paper_title: str, csv_file: str) -> bool:
    """
    Check if a paper with the given title exists in the CSV file.
    """
    with open(csv_file, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['title'] == paper_title:
                return True
    return False


def read_csv_links_and_referrers(file_path):
    # try to open file path and if it does not exist just return an empty list
    if not os.path.exists(file_path):
        return []
    with open(file_path, mode='r') as f:
        reader = csv.DictReader(f)
        return [(row['paper'], row['referrer']) for row in reader]


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


def background(f):
    """
    Decorator that turns a synchronous function into an asynchronous function by running it in an
    executor using the default event loop.

    Args:
        f (Callable): The function to be turned into an asynchronous function.

    Returns:
        Callable: The wrapped function that can be called asynchronously.
    """
    def wrapped(*args, **kwargs):
        """
        Wrapper function that calls the original function 'f' in an executor using the default event loop.

        Args:
            *args: Positional arguments to pass to the original function 'f'.
            **kwargs: Keyword arguments to pass to the original function 'f'.

        Returns:
            Any: The result of the original function 'f'.
        """
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


def authenticate_service_account(service_account_file: str) -> ServiceAccountCredentials:
    """Authenticates using service account and returns the session."""

    credentials = ServiceAccountCredentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/youtube.readonly"]
    )
    return credentials


def get_playlist_title(credentials: ServiceAccountCredentials, api_key: str, playlist_id: str) -> Optional[str]:
    """
    Retrieves the title of a YouTube playlist using the YouTube Data API.

    Args:
        api_key (str): Your YouTube Data API key.
        playlist_id (str): The YouTube playlist ID.

    Returns:
        Optional[str]: The title of the playlist if found, otherwise None.
    """
    # Initialize the YouTube API client
    if credentials is None:
        youtube = build('youtube', 'v3', developerKey=api_key)
    else:
        youtube = build('youtube', 'v3', credentials=credentials, developerKey=api_key)

    request = youtube.playlists().list(
        part='snippet',
        id=playlist_id,
        fields='items(snippet/title)',
        maxResults=1
    )
    response = request.execute()
    items = response.get('items', [])

    if items:
        return items[0]['snippet']['title']
    else:
        return None


def get_videos_from_playlist(credentials: ServiceAccountCredentials, api_key: str, playlist_id: str, max_results: int = 5000) -> List[dict]:
    # Initialize the YouTube API client
    if credentials is None:
        youtube = build('youtube', 'v3', developerKey=api_key)
    else:
        youtube = build('youtube', 'v3', credentials=credentials, developerKey=api_key)

    video_info = []
    next_page_token = None

    while True:
        playlist_request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=max_results,
            pageToken=next_page_token,
            fields="nextPageToken,items(snippet(publishedAt,resourceId(videoId),title))"
        )
        playlist_response = playlist_request.execute()
        items = playlist_response.get('items', [])

        for item in items:
            video_id = item["snippet"]["resourceId"]["videoId"]
            video_info.append({
                'url': f'https://www.youtube.com/watch?v={video_id}',
                'id': video_id,
                'title': item["snippet"]["title"],
                'publishedAt': item["snippet"]["publishedAt"]
            })

        next_page_token = playlist_response.get("nextPageToken")

        if next_page_token is None or len(video_info) >= max_results:
            break

    return video_info


def get_root_directory():
    current_dir = os.getcwd()

    while True:
        if '.git' in os.listdir(current_dir):
            return current_dir
        else:
            # Go up one level
            current_dir = os.path.dirname(current_dir)


def get_channel_name(api_key, channel_handle):
    youtube = build('youtube', 'v3', developerKey=api_key)

    request = youtube.search().list(
        part='snippet',
        type='channel',
        q=channel_handle,
        maxResults=1,
        fields='items(snippet(channelTitle))'
    )
    response = request.execute()

    if response['items']:
        return response['items'][0]['snippet']['channelTitle']
    else:
        return None


def load_channel_ids(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    else:
        return {}


def save_channel_ids(filepath, channel_name_to_id):
    with open(filepath, 'w', encoding='utf-8') as file:
        json.dump(channel_name_to_id, file, ensure_ascii=False, indent=4)


async def get_channel_id(session, api_key, channel_name, channel_name_to_id):
    if channel_name in channel_name_to_id:
        return channel_name_to_id[channel_name]

    url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=1&q={channel_name}&key={api_key}"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            items = data.get('items', [])
            if items:
                channel_id = items[0]['id']['channelId']
                channel_name_to_id[channel_name] = channel_id  # Update the mapping
                return channel_id
        return None  # Handle errors or missing data as appropriate for your application


def return_driver(headless=False):
    # set up Chrome driver options
    options = webdriver.ChromeOptions()
    # options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_argument("--start-maximized")
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--remote-debugging-port=9222")
    # options.add_argument("--disable-gpu")
    # options.add_argument("--disable-features=IsolateOrigins,site-per-process")

    # # Add headless option if required
    if headless:
        options.add_argument("--headless")

    # options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # NOTE: ChromeDriverManager().install() no longer works
    # needed to manually go here https://googlechromelabs.github.io/chrome-for-testing/#stable
    # and provide direct paths to script for both binary and driver
    # First run the script get_correct_chromedriver.sh
    # Paths for the Chrome binary and ChromeDriver
    # TODO 2023-09-18: add GIT LFS unroll of chromium folder when hitting this script
    CHROME_BINARY_PATH = f'{root_directory()}/src/chromium/chrome-linux64/chrome'
    CHROMEDRIVER_PATH = f'{root_directory()}/src/chromium/chromedriver-linux64/chromedriver'

    # options = webdriver.ChromeOptions()
    options.binary_location = CHROME_BINARY_PATH

    driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, chrome_options=options)
    return driver


def return_driver_get_discourse(headless=False):
    # set up Chrome driver options
    options = webdriver.ChromeOptions()
    # options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--remote-debugging-port=9222")
    # options.add_argument("--disable-gpu")
    # options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_experimental_option('useAutomationExtension', False)

    # # Add headless option if required
    if headless:
        options.add_argument("--headless")

    # options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # NOTE: ChromeDriverManager().install() no longer works
    # needed to manually go here https://googlechromelabs.github.io/chrome-for-testing/#stable
    # and provide direct paths to script for both binary and driver
    # First run the script get_correct_chromedriver.sh
    # Paths for the Chrome binary and ChromeDriver
    # TODO 2023-09-18: add GIT LFS unroll of chromium folder when hitting this script
    CHROME_BINARY_PATH = f'{root_directory()}/src/chromium/chrome-linux64/chrome'
    CHROMEDRIVER_PATH = f'{root_directory()}/src/chromium/chromedriver-linux64/chromedriver'

    # options = webdriver.ChromeOptions()
    options.binary_location = CHROME_BINARY_PATH

    driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, chrome_options=options)
    return driver

def create_directory(directory):
    os.makedirs(directory, exist_ok=True)


def download_pdf(pdf_link, original_url, retries=0, delay=1):
    if retries >= MAX_RETRIES:
        return None

    headers = {
        'User-Agent': choice(USER_AGENTS),
        'Referer': original_url
    }

    session = Session()

    try:
        response = session.get(pdf_link, headers=headers)
        response.raise_for_status()

        if response.headers['Content-Type'] == 'application/pdf':
            return response.content

    except requests.RequestException as e:
        if response.status_code == 429:  # Too Many Requests
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                delay = int(retry_after)  # Use the Retry-After header for delay if present
            else:
                delay = min(2 * delay, MAX_DELAY)  # Double the delay, but do not exceed MAX_DELAY

            logging.warning(f"[{pdf_link}] Rate limited. Retrying in {delay} seconds...")
            time.sleep(delay)
            return download_pdf(pdf_link, original_url, retries + 1, delay)
        else:
            logging.error(f"Failed to download PDF from {pdf_link}. Error: {e}")
            return None


def download_and_save_unique_paper(args):
    """
    Download a paper from its link and save its details to a CSV file.

    Parameters:
    - args (tuple): A tuple containing the following elements:
        - paper_site (str): The website where the paper is hosted.
        - link (str): Direct link to the paper's details page (not the PDF link).
        - csv_file (str): Path to the CSV file where details should be saved.
        - existing_papers (list): List of existing paper titles in the directory.
        - headers (dict): Headers to be used in the HTTP request to fetch paper details.
        - referrer (str): Referrer URL or identifier to be stored alongside paper details.
        - parsing_method (function): The function to use for parsing the paper details from the webpage.
    """
    paper_site, link, csv_file, existing_papers, headers, referrer, parsing_method = args
    paper_page_url = link.replace('.pdf', '')
    paper_details = parsing_method(paper_page_url)

    if paper_details is None:
        logging.error(f"[{paper_site}] Failed to fetch details for {paper_page_url}")
        return

    # Append details to CSV if paper does not exist in CSV
    if not paper_exists_in_csv(paper_details['title'], csv_file):
        # Ensure CSV ends with a newline
        ensure_newline_in_csv(csv_file)

        paper_details["referrer"] = referrer

        with open(csv_file, 'a', newline='') as csvfile:
            fieldnames = ['title', 'authors', 'pdf_link', 'topics', 'release_date', 'referrer']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(paper_details)

    # Define the potential file path
    pdf_filename = f"{paper_details['title']}.pdf"
    pdf_path = os.path.join(root_directory(), 'data', 'papers_pdf_downloads', pdf_filename)

    # If PDF does not exist, download it
    if not os.path.exists(pdf_path):
        try:
            pdf_content = download_pdf(paper_details['pdf_link'], '')
            if pdf_content:
                with open(pdf_path, "wb") as f:
                    f.write(pdf_content)
                logging.info(f"[{paper_site}] Downloaded paper {pdf_filename}")
        except Exception as e:
            logging.error(f"Failed to download a valid PDF file from {link} after multiple attempts. Error: {e}")


def download_and_save_paper(paper_site, paper_links_and_referrers, csv_file, parsing_method):
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

    # Create a list of tuples where each tuple contains the arguments for a single call
    # to download_and_save_unique_paper
    tasks = [
        (
            paper_site,
            link,
            csv_file,
            existing_papers,
            headers,
            referrer,
            parsing_method
        )
        for link, referrer in paper_links_and_referrers
    ]

    # Use ProcessPoolExecutor to run tasks in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(download_and_save_unique_paper, tasks)


def validate_pdfs(directory_path: Union[str, Path]):
    if not isinstance(directory_path, Path):
        directory_path = Path(directory_path)

    for pdf_file in directory_path.glob("*.pdf"):
        try:
            # Try to open and read the PDF
            with open(pdf_file, "rb") as file:
                reader = PyPDF2.PdfReader(file)
                # Get the number of pages in the PDF just as a basic check
                num_pages = len(reader.pages)
        except Exception as e:
            logging.info(f"Invalid PDF {pdf_file}, deleting... Reason: {e}")
            os.remove(pdf_file)

