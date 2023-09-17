import concurrent.futures
import csv
import os
import re
import logging
from datetime import datetime

from googleapiclient.discovery import build
from src.utils import root_directory, authenticate_service_account


# Function to extract video ID from a YouTube link
def extract_video_id_from_link(video_link):
    # Extract the video ID from the YouTube link
    video_id_match = re.match(r'^https:\/\/www\.youtube\.com\/watch\?v=([a-zA-Z0-9_-]+)', video_link)
    if video_id_match:
        return video_id_match.group(1)
    return None


# Function to fetch video details by video link
def get_video_details_by_link(credentials, api_key, video_link):
    # Extract video ID from the video link
    video_id = extract_video_id_from_link(video_link)

    if video_id:
        # Initialize the YouTube API client
        if credentials is None:
            youtube = build('youtube', 'v3', developerKey=api_key)
        else:
            youtube = build('youtube', 'v3', credentials=credentials, developerKey=api_key)

        # Request video details
        video_request = youtube.videos().list(
            part="snippet",
            id=video_id
        )

        try:
            video_response = video_request.execute()
            if video_response and 'items' in video_response:
                video_info = video_response['items'][0]['snippet']
                published_at = video_info['publishedAt']
                # Parse the timestamp to yyyy-mm-dd format
                parsed_published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")

                return {
                    'title': video_info['title'],
                    'channel_name': video_info['channelTitle'],
                    'Publish date': parsed_published_at.strftime("%Y-%m-%d"),
                }
        except Exception as e:
            logging.error(f"Error occurred while fetching video details. Error: {e}")

    return None


# Function to process a single row of the CSV and fetch video details
def process_csv_row(row):
    video_link = row['video']
    referrer_link = row['referrer']
    video_details = get_video_details_by_link(credentials, api_key, video_link)
    if video_details:
        video_details['referrer'] = referrer_link
        video_details['link'] = video_link
    return video_details


if __name__ == '__main__':
    api_key = os.environ.get('YOUTUBE_API_KEY')
    if not api_key:
        raise ValueError("No API key provided. Please provide an API key via command line argument or .env file.")

    service_account_file = os.environ.get('SERVICE_ACCOUNT_FILE')
    credentials = None

    if service_account_file:
        credentials = authenticate_service_account(service_account_file)
        print("\nService account file found. Proceeding with public channels, playlists, or private videos if accessible via Google Service Account.")
    else:
        print("\nNo service account file found. Proceeding with public channels or playlists.")

    csv_file_path = os.path.join(root_directory(), 'data/links/recommended_youtube_videos.csv')

    # Read the CSV file
    video_channels = []
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            video_channels.append(row)

    # Process video channels in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        video_details_list = list(executor.map(process_csv_row, video_channels))

    # Filter out None values (failed requests)
    video_details_list = [video for video in video_details_list if video]

    # Print or save video details as needed
    output_csv_file_path = os.path.join(root_directory(), 'data/links/recommended_youtube_videos_with_details.csv')
    headers = ['title', 'channel_name', 'Publish date', 'link', 'referrer']

    with open(output_csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()  # Write the header

        for video_details in video_details_list:
            # Write video details to the CSV file
            writer.writerow({
                'title': video_details['title'],
                'channel_name': video_details['channel_name'],
                'Publish date': video_details['Publish date'],
                'link': video_details['link'],
                'referrer': video_details['referrer'],
            })

    print(f"Results have been written to {output_csv_file_path}")
