import asyncio
import os
from typing import List, Optional, Dict
from googleapiclient.discovery import build
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from datetime import datetime
import csv


from src.utils import root_directory


# Load environment variables from the .env file
load_dotenv()


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


def get_video_info(credentials: ServiceAccountCredentials, api_key: str, channel_id: str, channel_name: str, max_results: int = 500000) -> List[dict]:
    """
    Retrieves video information (URL, ID, and title) from a YouTube channel using the YouTube Data API.

    Args:
        api_key (str): Your YouTube Data API key.
        channel_id (str): The YouTube channel ID.
        max_results (int, optional): Maximum number of results to retrieve. Defaults to 50.

    Returns:
        list: A list of dictionaries containing video URL, ID, and title from the channel.
    """
    # Initialize the YouTube API client
    if credentials is None:
        youtube = build('youtube', 'v3', developerKey=api_key)
    else:
        youtube = build('youtube', 'v3', credentials=credentials, developerKey=api_key)

    # Get the "Uploads" playlist ID
    channel_request = youtube.channels().list(
        part="contentDetails",
        id=channel_id,
        fields="items/contentDetails/relatedPlaylists/uploads"
    )
    channel_response = channel_request.execute()
    uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Fetch videos from the "Uploads" playlist
    video_info = []
    next_page_token = None

    while True:
        playlist_request = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=max_results,
            pageToken=next_page_token,
            fields="nextPageToken,items(snippet(publishedAt,resourceId(videoId),title))"
        )
        try:
            playlist_response = playlist_request.execute()
        except Exception as e:
            print(f"Error occurred while fetching videos from the channel. Error: {e}")
            return video_info
        items = playlist_response.get('items', [])

        for item in items:
            video_id = item["snippet"]["resourceId"]["videoId"]
            published_at = item["snippet"]["publishedAt"]

            # Parse the timestamp to yyyy-mm-dd format
            parsed_published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
            video_info.append({
                'name': item["snippet"]["title"],
                'channel_name': channel_name,
                'published_date': parsed_published_at.strftime("%Y-%m-%d"),
                'url': f'https://www.youtube.com/watch?v={video_id}',
                'id': video_id,
            })


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

def get_channel_id(credentials: Optional[ServiceAccountCredentials], api_key: str, channel_name: str) -> Optional[str]:
    """
    Get the channel ID of a YouTube channel by its name.

    Args:
        api_key (str): Your YouTube Data API key.
        channel_name (str): The name of the YouTube channel.

    Returns:
        Optional[str]: The channel ID if found, otherwise None.
    """
    # Initialize the YouTube API client
    if credentials is None:
        youtube = build('youtube', 'v3', developerKey=api_key)
    else:
        youtube = build('youtube', 'v3', credentials=credentials, developerKey=api_key)

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


def run(api_key: str, yt_channels: Optional[List[str]] = None, yt_playlists: Optional[List[str]] = None, keywords: [List[str]] = None):
    """
    Run function that takes a YouTube Data API key and a list of YouTube channel names, fetches video transcripts,
    and saves them as .txt files in a data directory.

    Args:
        yt_playlists:
        api_key (str): Your YouTube Data API key.
        yt_channels (List[str]): A list of YouTube channel names.
        :param keywords:
    """
    service_account_file = os.environ.get('SERVICE_ACCOUNT_FILE')
    credentials = None

    if service_account_file:
        credentials = authenticate_service_account(service_account_file)
        print("\nService account file found. Proceeding with public channels, playlists, or private videos if accessible via Google Service Account.")
    else:
        print("\nNo service account file found. Proceeding with public channels or playlists.")

    # Create a list to store video information
    video_info_list = []

    if yt_channels:
        # Iterate through the list of channel names
        for channel_name in yt_channels:
            # Get channel ID from channel name
            channel_id = get_channel_id(credentials=credentials, api_key=api_key, channel_name=channel_name)

            if channel_id:
                # Get video information from the channel
                video_info_list.extend(get_video_info(credentials, api_key, channel_id, channel_name))

    if yt_playlists:
        for playlist_id in yt_playlists:
            video_info_list.extend(get_videos_from_playlist(credentials, api_key, playlist_id))

    # Save video information as a CSV file
    csv_file_path = f"{root_directory()}/data/links/youtube_videos.csv"
    existing_data = []
    existing_video_names = set()

    if os.path.exists(csv_file_path):
        with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                cleaned_row = {k.strip(): v.strip() for k, v in row.items()}
                existing_data.append(cleaned_row)
                existing_video_names.add(cleaned_row['name'])

    write_header = not os.path.exists(csv_file_path)

    # Cleaning the keys and values of video info list
    video_info_list = [
        {k.strip(): v.strip() for k, v in video_info.items()}
        for video_info in video_info_list
    ]

    # Removing duplicates based on video names
    video_info_list = [
        video_info for video_info in video_info_list
        if video_info['name'] not in existing_video_names
    ]

    # Combine new and existing data
    all_video_data = existing_data + video_info_list

    # Create filtered list and filtered away list based on keywords
    filtered_video_info_list = [
        video_info for video_info in all_video_data
        if any(keyword.lower() in video_info['name'].lower() for keyword in keywords)
    ]

    filtered_away_video_info_list = [
        video_info for video_info in all_video_data
        if not any(keyword.lower() in video_info['name'].lower() for keyword in keywords)
    ]

    # Define your headers here
    headers = ['link', 'id', 'name', 'publish date']

    # Write filtered video info list to csv
    with open(csv_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)

        if write_header:
            writer.writeheader()

        for video_info in filtered_video_info_list:
            writer.writerow(video_info)

    # Write filtered away video info list to a new csv file
    filtered_away_csv_file_path = f"{root_directory()}/data/links/filtered_away_youtube_videos.csv"

    # Check if the filtered away csv file already exists
    write_filtered_away_header = not os.path.exists(filtered_away_csv_file_path)

    with open(filtered_away_csv_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)

        if write_filtered_away_header:
            writer.writeheader()

        for video_info in filtered_away_video_info_list:
            writer.writerow(video_info)


# Function to read the channel handles from a file
def get_youtube_channels_from_file(file_path):
    with open(file_path, 'r') as file:
        channels = file.read().split(',')
    return channels


if __name__ == '__main__':
    api_key = os.environ.get('YOUTUBE_API_KEY')
    if not api_key:
        raise ValueError("No API key provided. Please provide an API key via command line argument or .env file.")

    yt_channels_file = os.path.join(root_directory(), 'data/links/youtube_channel_handles.txt')

    # Fetch the yt_channels from the file
    yt_channels = get_youtube_channels_from_file(yt_channels_file)

    yt_playlists = os.environ.get('YOUTUBE_PLAYLISTS')
    if yt_playlists:
        yt_playlists = [playlist.strip() for playlist in yt_playlists.split(',')]

    if not yt_channels and not yt_playlists:
        raise ValueError(
            "No channels or playlists provided. Please provide channel names, IDs, or playlist IDs via command line argument or .env file.")

    # Note, the use of keywords List is an attempt at filtering YouTube videos by name content to reduce noise
    keywords = ['order flow', 'transaction', 'mev', 'ordering', 'sgx', 'intent', 'dex', 'front-running', 'arbitrage',
                'maximal extractable value', 'games', 'timing', 'onc0chain games', 'pepc', 'proposer', 'builder', 'barnabe',
                'fees', 'pbs', '4337', 'account abstraction', 'wallet', 'boost', 'defi', 'uniswap', 'hook', 'anoma', 'espresso',
                'suave', 'flashbots', 'celestia', 'gas war', 'hasu', 'dan robinson', 'jon charbonneau', 'robert miller', 'paradigm',
                'altlayer', 'tarun', 'modular summit', 'latency', 'market design', 'searcher', 'staking', 'pre-merge', 'post-merge',
                'liquid staking', 'crediblecommitments', 'tee', 'market microstructure', 'research', 'rollups', 'uniswap', '1inch',
                'cow', 'censorship', 'liquidity', 'censorship', 'ofa', 'pfof', 'payment for order flow', 'decentralisation', 'decentralization', 'bridge', 'evm',
                'eth global', 'zk', 'erc', 'eip']
    run(api_key, yt_channels, yt_playlists, keywords)

