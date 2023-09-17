import os
import shutil
import tempfile
import traceback
import random
from typing import List, Optional
import json
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from datetime import datetime
import logging
import csv
import asyncio
from aiohttp import ClientSession


from src.utils import root_directory, authenticate_service_account, get_videos_from_playlist, get_channel_id, get_channel_name

# Load environment variables from the .env file
load_dotenv()

# Note, the use of keywords List is an attempt at filtering YouTube videos by name content to reduce noise
keywords = ['order flow', 'orderflow', 'transaction', 'mev', 'ordering', 'sgx', 'intent', 'dex', 'front-running', 'arbitrage', 'back-running',
            'maximal extractable value', 'trading games', 'timing games', 'arbitrage games', 'timing', 'on-chain games', 'pepc', 'proposer', 'builder', 'barnabe',
            'fees', 'pbs', '4337', 'account abstraction', 'boost', 'defi', 'uniswap', 'hook', 'anoma', 'espresso',
            'suave', 'flashbots', 'celestia', 'gas war', 'hasu', 'dan robinson', 'jon charbonneau', 'robert miller', 'paradigm',
            'altlayer', 'tarun', 'modular summit', 'latency', 'market design', 'searcher', 'staking', 'pre-merge', 'post-merge',
            'liquid staking', 'crediblecommitments', 'tee', 'market microstructure', 'rollups', 'uniswap', '1inch',
            'cow', 'censorship', 'liquidity', 'censorship', 'ofa', 'pfof', 'payment for order flow', 'decentralisation', 'decentralization',
            'erc', 'eip', 'auction', 'daian', 'mechanism design', 'Price-of-Anarchy', 'protocol economics', 'stephane gosselin', 'su zhu']
            # , 'smart contract', 'eth global',  'evm',  #  'vitalik', 'buterin', bridge',

keywords_to_exclude = ['DAO', 'NFTs', 'joke', 'jokes', 'short', 'shorts', '#', 'gensler', 'sec']


async def get_video_details(youtube, video_id, keywords, keywords_to_exclude):
    video_request = youtube.videos().list(
        part="snippet",
        id=video_id
    )

    try:
        video_response = video_request.execute()
        video_info_item = video_response['items'][0]['snippet']
        video_title = video_info_item['title']

        # Check if the video title contains any of the keywords
        if any(keyword.lower() in video_title.lower() for keyword in keywords) and not any(keyword.lower() in video_title.lower() for keyword in keywords_to_exclude):
            published_at = video_info_item['publishedAt']
            parsed_published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
            logging.info(f"[{video_info_item['channelTitle']}] added video: [{video_title}]")
            return {
                'title': video_title,
                'channel_name': video_info_item['channelTitle'],
                'published_date': parsed_published_at.strftime("%Y-%m-%d"),
                'url': f'https://www.youtube.com/watch?v={video_id}',
            }
        else:
            # logging.info(f"[{video_info_item['channelTitle']}] Skipped video: [{video_title}] (Title doesn't match keyword criteria)")
            return None
    except Exception as e:
        logging.error(f"[{youtube}] and id [{video_id}] Error occurred while fetching video details. Error: {e}")
        traceback.print_exc()
        return None


async def get_video_info(session, credentials: ServiceAccountCredentials, api_key: str, channel_id: str, channel_name: str, max_results: int = 50) -> List[dict]:
    """
    Retrieves video information (URL, ID, title, and published date) from a YouTube channel using the YouTube Data API.

    Args:
        api_key (str): Your YouTube Data API key.
        channel_id (str): The YouTube channel ID.
        max_results (int, optional): Maximum number of results to retrieve. Defaults to 50.

    Returns:
        list: A list of dictionaries containing video URL, ID, title, and published date from the channel.
    """
    # Initialize the YouTube API client
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
        )
        try:
            playlist_response = playlist_request.execute()
        except Exception as e:
            logging.error(f"Error occurred while fetching videos from the channel. Error: {e}")
            return video_info
        video_ids = [item["snippet"]["resourceId"]["videoId"] for item in playlist_response.get('items', [])]
        # logging.info(f"[{channel_name}] Fetched video IDs from the channel: {video_ids}")
        # Fetch video details in parallel
        video_details = await asyncio.gather(*[get_video_details(youtube, video_id, keywords, keywords_to_exclude) for video_id in video_ids])

        video_info.extend([video for video in video_details if video])  # Extend the list instead of overwriting it

        next_page_token = playlist_response.get('nextPageToken')

        if not next_page_token:
            break

    return video_info


def save_video_info_to_csv(video_info_list, csv_file_path, existing_video_names, headers):
    # Remove duplicates based on video titles
    video_info_list = [
        video_info for video_info in video_info_list
        if video_info['title'] not in existing_video_names
    ]

    # Append new data to the CSV file
    with open(csv_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers, quoting=csv.QUOTE_MINIMAL)  # Add quoting option
        for video_info in video_info_list:
            # Ensure titles are enclosed in double quotes
            video_info['title'] = f'"{video_info["title"]}"'
            writer.writerow(video_info)


async def fetch_and_save_channel_videos(session, channel_id, channel_name, credentials, api_key, csv_file_path, existing_video_names, headers):
    video_info_list = await get_video_info(session, credentials, api_key, channel_id, channel_name)

    save_video_info_to_csv(video_info_list, csv_file_path, existing_video_names, headers)
    logging.info(f"[{channel_name}] Saved {len(video_info_list)} videos to CSV file {csv_file_path}.")


async def fetch_and_save_channel_videos_async(session, youtube, channel_id, channel_name, credentials, api_key, csv_file_path, existing_video_names, headers):
    video_info_list = await get_video_info(session, credentials, api_key, channel_id, channel_name)

    save_video_info_to_csv(video_info_list, csv_file_path, existing_video_names, headers)
    logging.info(f"[{channel_name}] Saved {len(video_info_list)} videos to CSV file {csv_file_path}.")


async def fetch_youtube_videos(api_key, yt_channels, yt_playlists, keywords, keywords_to_exclude, fetch_videos):
    """
    Fetches YouTube video information from specified channels and playlists, and optionally filters them based on keywords.

    Args:
        api_key (str): Your YouTube Data API key.
        yt_channels (Optional[List[str]]): List of YouTube channel names or IDs to fetch videos from.
        yt_playlists (Optional[List[str]]): List of YouTube playlist IDs to fetch videos from.
        keywords (List[str]): Keywords to filter videos by.
        keywords_to_exclude (List[str]): Keywords to exclude videos by.
        fetch_videos (bool): Whether to fetch videos or not.

    Returns:
        List[dict]: A list of dictionaries containing video information.
    """
    service_account_file = os.environ.get('SERVICE_ACCOUNT_FILE')
    credentials = None

    if service_account_file:
        credentials = authenticate_service_account(service_account_file)
        logging.info("\nService account file found. Proceeding with public channels, playlists, or private videos if accessible via Google Service Account.")
    else:
        logging.info("\nNo service account file found. Proceeding with public channels or playlists.")

    csv_file_exists, csv_file_path, headers = setup_csv()

    existing_data, existing_video_names, existing_channel_names = load_existing_data(csv_file_exists, csv_file_path)

    channel_handle_to_name = get_channel_names(api_key, yt_channels)

    channels_in_csv, channels_not_in_csv = separate_channels_based_on_csv(channel_handle_to_name, existing_channel_names, yt_channels)

    if fetch_videos:
        await fetch_channel_videos(api_key, channel_handle_to_name, channels_in_csv, channels_not_in_csv, credentials, csv_file_path, existing_video_names, headers, yt_channels)

        await fetch_playlist_videos(api_key, credentials, csv_file_path, existing_video_names, headers, yt_playlists)

    return existing_data


async def fetch_playlist_videos(api_key, credentials, csv_file_path, existing_video_names, headers, yt_playlists):
    if yt_playlists:
        for playlist_id in yt_playlists:
            video_info_list = get_videos_from_playlist(credentials, api_key, playlist_id)
            save_video_info_to_csv(video_info_list, csv_file_path, existing_video_names, headers)


async def fetch_channel_videos(api_key, channel_handle_to_name, channels_in_csv, channels_not_in_csv, credentials, csv_file_path, existing_video_names, headers, yt_channels):
    if yt_channels:
        async with ClientSession() as session:
            youtube = build('youtube', 'v3', developerKey=api_key)
            # Fetching videos from channels not in the CSV file first
            for channel_handle in channels_not_in_csv:
                channel_name = channel_handle_to_name.get(channel_handle)
                if channel_name:
                    channel_id = get_channel_id(youtube, channel_name)
                    if channel_id is not None:
                        await fetch_and_save_channel_videos_async(session, youtube, channel_id, channel_name, credentials, api_key, csv_file_path, existing_video_names, headers)

            # Fetching videos from channels already in the CSV file
            for channel_handle in channels_in_csv:
                channel_name = channel_handle_to_name.get(channel_handle)
                if channel_name:
                    channel_id = get_channel_id(youtube, channel_name)
                    if channel_id is not None:
                        await fetch_and_save_channel_videos_async(session, youtube, channel_id, channel_name, credentials, api_key, csv_file_path, existing_video_names, headers)


def separate_channels_based_on_csv(channel_handle_to_name, existing_channel_names, yt_channels):
    # Creating two separate lists for channels: one for channels not in the CSV file,
    # and another for channels already in the CSV file
    channels_not_in_csv = []
    channels_in_csv = []
    for channel_handle in yt_channels:
        channel_name = channel_handle_to_name.get(channel_handle)
        if channel_name:
            if channel_name in existing_channel_names:
                channels_in_csv.append(channel_handle)
            else:
                channels_not_in_csv.append(channel_handle)
    return channels_in_csv, channels_not_in_csv


def get_channel_names(api_key, yt_channels):
    mapping_filepath = os.path.join(root_directory(), "data/links/channel_handle_to_name_mapping.json")

    # Check if mapping file exists, if so, load it
    if os.path.exists(mapping_filepath):
        with open(mapping_filepath, 'r', encoding='utf-8') as file:
            channel_handle_to_name = json.load(file)
    else:
        channel_handle_to_name = {}

    # Check if any channel handles are missing in the loaded/existing mapping, and fetch those
    missing_handles = [handle for handle in yt_channels if handle not in channel_handle_to_name]

    for channel_handle in missing_handles:
        channel_name = get_channel_name(api_key, channel_handle)
        if channel_name:
            channel_handle_to_name[channel_handle] = channel_name
            logging.info(f"[{channel_handle}] added channel name: {channel_name}")

    # Save the updated mapping to file
    with open(mapping_filepath, 'w', encoding='utf-8') as file:
        json.dump(channel_handle_to_name, file, ensure_ascii=False, indent=4)

    return channel_handle_to_name


def load_existing_data(csv_file_exists, csv_file_path):
    # Create a DataFrame to store existing data read from the CSV file and create sets of existing video names and channel names
    if csv_file_exists:
        existing_data_df = pd.read_csv(csv_file_path, encoding='utf-8')

        # Create a set for existing video names for faster lookup
        existing_video_names = set(existing_data_df['title'].str.strip())

        # Create a set for existing channel names for faster lookup
        existing_channel_names = set(existing_data_df['channel_name'].str.strip())

        # Convert the DataFrame back to a list of dictionaries to create existing_data
        existing_data = existing_data_df.to_dict('records')
    else:
        # If the CSV file does not exist, initialize empty sets and list
        existing_data = []
        existing_video_names = set()
        existing_channel_names = set()

    return existing_data, existing_video_names, existing_channel_names


def setup_csv():
    # Check if the CSV file already exists
    csv_file_path = f"{root_directory()}/data/links/youtube_videos.csv"
    csv_file_exists = os.path.exists(csv_file_path)
    headers = ['title', 'channel_name', 'published_date', 'url', 'referrer']
    # Check if the CSV file exists and if its headers match the expected headers
    if csv_file_exists:
        existing_data_df = pd.read_csv(csv_file_path, encoding='utf-8', nrows=0)  # Read just the header
        existing_headers = existing_data_df.columns.tolist()

        if existing_headers != headers:
            # Create a new CSV file with the specified headers
            pd.DataFrame(columns=headers).to_csv(csv_file_path, index=False, encoding='utf-8')
    else:
        # Create a new CSV file with the specified headers
        pd.DataFrame(columns=headers).to_csv(csv_file_path, index=False, encoding='utf-8')
    return csv_file_exists, csv_file_path, headers


def filter_and_save_video_info(existing_data, keywords, csv_file_path):
    video_info_list = existing_data

    video_info_list = [
        {k.strip(): v.strip() for k, v in video_info.items()}
        for video_info in video_info_list
    ]

    existing_video_names = set(video_info['title'] for video_info in video_info_list)

    headers = ['link', 'id', 'title', 'publish date']

    filtered_video_info_list = [
        video_info for video_info in video_info_list
        if any(keyword.lower() in video_info['title'].lower() for keyword in keywords)
    ]

    filtered_away_video_info_list = [
        video_info for video_info in video_info_list
        if not any(keyword.lower() in video_info['title'].lower() for keyword in keywords)
    ]

    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()

        for video_info in filtered_video_info_list:
            writer.writerow(video_info)

    filtered_away_csv_file_path = f"{root_directory()}/data/links/filtered_away_youtube_videos.csv"
    write_filtered_away_header = not os.path.exists(filtered_away_csv_file_path)

    with open(filtered_away_csv_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)

        if write_filtered_away_header:
            writer.writeheader()

        for video_info in filtered_away_video_info_list:
            writer.writerow(video_info)


def filter_and_remove_videos(input_csv_path, keywords):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv_path)

    # Filter the DataFrame based on keywords
    filtered_df = df[df['title'].str.lower().str.contains('|'.join(keywords).lower())]

    # Identify removed videos
    removed_df = df[~df['title'].isin(filtered_df['title'])]

    # Log the removed video titles
    for _, removed_video in removed_df.iterrows():
        logging.info(f"Removed video: {removed_video['title']}")

    # Write the filtered data back to the CSV file
    filtered_df.to_csv(input_csv_path, index=False)


async def run(api_key: str, yt_channels: Optional[List[str]] = None, yt_playlists: Optional[List[str]] = None,
              keywords: List[str] = None, keywords_to_exclude: List[str] = None, fetch_videos: bool = True):
    csv_file_path = f"{root_directory()}/data/links/youtube_videos.csv"
    existing_data = await fetch_youtube_videos(api_key, yt_channels, yt_playlists, keywords, keywords_to_exclude, fetch_videos)
    filter_and_save_video_info(existing_data, keywords, csv_file_path)


# Function to read the channel handles from a file
def get_youtube_channels_from_file(file_path):
    with open(file_path, 'r') as file:
        channels = file.read().split(',')
    return channels


if __name__ == '__main__':
    # TODO 2023-09-11: add functionality to only load the difference between the existing data and the new data, expectedly being able to see only videos from a given timestamp and on
    # TODO 2023-09-11: add functionality to fetch all videos which are unlisted
    fetch_videos = False
    if not fetch_videos:
        logging.info(f"Applying new filters only, not fetching videos.")

    if fetch_videos:
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

        asyncio.run(run(api_key, yt_channels, yt_playlists, keywords, keywords_to_exclude, fetch_videos=True))

    # Specify the input and output CSV file paths
    input_csv_path = f"{root_directory()}/data/links/youtube_videos.csv"
    output_csv_path = f"{root_directory()}/data/links/filtered_and_updated_youtube_videos.csv"

    # Call the filter_and_log_removed_videos method to filter and log removed videos
    filter_and_remove_videos(input_csv_path, keywords)
