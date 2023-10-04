import asyncio
import csv
import logging
import os
from typing import Optional, List

import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from selenium import webdriver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def root_directory() -> str:
    """
    Determine the root directory of the project based on the presence of '.git' directory.

    Returns:
    - str: The path to the root directory of the project.
    """
    current_dir = os.getcwd()

    while True:
        if '.git' in os.listdir(current_dir):
            return current_dir
        else:
            # Go up one level
            current_dir = os.path.dirname(current_dir)


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


def get_channel_id(youtube, channel_name: str) -> Optional[str]:
    """
    Get the channel ID of a YouTube channel by its name.

    Args:
        api_key (str): Your YouTube Data API key.
        channel_name (str): The name of the YouTube channel.

    Returns:
        Optional[str]: The channel ID if found, otherwise None.
    """

    # Create a search request to find the channel by name
    request = youtube.search().list(
        part='snippet',
        type='channel',
        q=channel_name,
        maxResults=1,
        fields='items(id(channelId))'
    )

    # Execute the request and get the response
    response = request.execute()

    # Get the list of items (channels) from the response
    items = response.get('items', [])

    # If there is at least one item, return the channel ID, otherwise return None
    if items:
        return items[0]['id']['channelId']
    else:
        return None


def return_driver():
    # set up Chrome driver options
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # NOTE: ChromeDriverManager().install() no longer works
    # needed to manually go here https://googlechromelabs.github.io/chrome-for-testing/#stable
    # and provide direct paths to script for both binary and driver
    # First run the script get_correct_chromedriver.sh
    # Paths for the Chrome binary and ChromeDriver
    # TODO 2023-09-18: add GIT LFS unroll of chromium folder when hitting this script
    CHROME_BINARY_PATH = f'{root_directory()}/src/chromium/chrome-linux64/chrome'
    CHROMEDRIVER_PATH = f'{root_directory()}/src/chromium/chromedriver-linux64/chromedriver'

    options = webdriver.ChromeOptions()
    options.binary_location = CHROME_BINARY_PATH

    driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, chrome_options=options)
    return driver
