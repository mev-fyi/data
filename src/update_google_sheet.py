import os
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread_dataframe
from dotenv import load_dotenv
import logging
import re

from bs4 import BeautifulSoup
import requests

from src.utils import root_directory

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_hyperlink_formula(value):
    if pd.isna(value) or value == "":
        return "anon"
    elif "http" not in value:
        return value
    elif 'twitter.com' in value:
        link_name = value.split('/')[-1]
        return f'=HYPERLINK("{value}", "{link_name}")'
    link_name = value
    max_length = 30  # Set the maximum length of the hyperlink text
    if len(link_name) > max_length:
        link_name = link_name[:max_length] + '...'  # Add ellipsis to indicate the text has been clipped
    return f'=HYPERLINK("{value}", "{link_name}")'


def get_title_from_url(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string
        return title
    except Exception as e:
        logging.error(f"Error fetching title for URL '{url}': {str(e)}")
        return ""


class GoogleSheetUpdater:
    def __init__(self, sheet_id, credentials_json):
        self.sheet_id = sheet_id
        self.credentials = Credentials.from_service_account_file(credentials_json, scopes=[
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive'
        ])
        self.client = gspread.authorize(self.credentials)

    def create_or_get_worksheet(self, tab_name, num_rows, num_cols):
        try:
            sheet = self.client.open_by_key(self.sheet_id).worksheet(tab_name)
            logging.info(f"Worksheet '{tab_name}' already exists.")
        except gspread.WorksheetNotFound:
            sheet = self.client.open_by_key(self.sheet_id).add_worksheet(
                title=tab_name,
                rows=num_rows,
                cols=num_cols
            )
            logging.info(f"Created new worksheet: '{tab_name}'.")
        return sheet

    def format_worksheet(self, sheet, df, tab_name):
        # Replace hyperlinks with titles using the create_hyperlink_formula function
        if 'link' in df.columns:
            df['link'] = df['link'].apply(create_hyperlink_formula)

        if 'referrer' in df.columns:
            df['referrer'] = df['referrer'].apply(create_hyperlink_formula)

        if tab_name == 'Articles':
            # Rename the columns to your desired names
            # title,article,referrer,release_date,authors
            column_mapping = {
                'title': 'Title',
                'authors': 'Authors',
                'article': 'Article',
                'release_date': 'Release date',
                'referrer': 'Referrer'
            }
            df.rename(columns=column_mapping, inplace=True)
            df['Article'] = df['Article'].apply(create_hyperlink_formula)

            # Reorder columns as per column_mapping
            df = df[list(column_mapping.values())]

            # Sort DataFrame in descending order by 'Release date' if the tab is 'Articles'
            df['Release date'] = pd.to_datetime(df['Release date'], errors='coerce')
            df.sort_values('Release date', ascending=False, inplace=True, na_position='last')
            # Convert 'Release date' back to string in 'yyyy-mm-dd' format
            df['Release date'] = df['Release date'].dt.strftime('%Y-%m-%d')

        # Pascal case all columns names
        df.columns = [col[0].upper() + col[1:] for col in df.columns]

        set_with_dataframe(sheet, df, row=1, col=1, include_index=False, resize=True)

        service = build('sheets', 'v4', credentials=self.credentials)

        requests = [
            # Bold formatting request for header
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet.id,
                        'startRowIndex': 0,
                        'endRowIndex': 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True
                            },
                            'horizontalAlignment': 'CENTER'  # Center-align header text
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.bold,userEnteredFormat.horizontalAlignment'
                }
            },
            # Left-align content rows
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet.id,
                        'startRowIndex': 1,  # Start from the row after the header
                        'endRowIndex': df.shape[0] + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'LEFT'  # Left-align content
                        }
                    },
                    'fields': 'userEnteredFormat.horizontalAlignment'
                }
            },
            # Freeze header row request
            {
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet.id,
                        'gridProperties': {
                            'frozenRowCount': 1
                        }
                    },
                    'fields': 'gridProperties.frozenRowCount'
                }
            }
        ]

        # Function to find the publish date column index
        def find_publish_or_release_date_column(df):
            # Define a regular expression pattern to match column names
            pattern = re.compile(r'publish(ed)?[_\s]?date', re.IGNORECASE)

            # Iterate through the column names
            for col_name in df.columns:
                if pattern.search(col_name):
                    return df.columns.get_loc(col_name)
            # Define a regular expression pattern to match column names
            pattern = re.compile(r'releas(ed)?[_\s]?date', re.IGNORECASE)

            # Iterate through the column names
            for col_name in df.columns:
                if pattern.search(col_name):
                    return df.columns.get_loc(col_name)
            return None
        # Usage
        publish_date_column_index = find_publish_or_release_date_column(df)
        if publish_date_column_index is not None:
            published_date = {
                'sortRange': {
                    'range': {
                        'sheetId': sheet.id,
                        'startRowIndex': 1,  # Start from the row after header
                        'endRowIndex': df.shape[0] + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': df.shape[1]
                    },
                    'sortSpecs': [{
                        'dimensionIndex': publish_date_column_index,
                        'sortOrder': 'DESCENDING'
                    }]
                }
            }
            requests.append(published_date)

        # Update the filter request to specify the exact range
        filter_request = {
            'setBasicFilter': {
                'filter': {
                    'range': {
                        'sheetId': sheet.id,
                        'startRowIndex': 0,  # Start from the header row
                        'endRowIndex': df.shape[0] + 1,  # Extend to the end of the data
                        'startColumnIndex': 0,
                        'endColumnIndex': df.shape[1]
                    }
                }
            }
        }

        # Append the filter request to the existing list of requests
        requests.append(filter_request)

        body = {
            'requests': requests
        }

        service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()

        num_columns = df.shape[1]
        resize_requests = []

        for col in range(num_columns):
            if df.columns[col].lower() != 'authors':
                resize_requests.append({
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": col,
                            "endIndex": col + 1
                        }
                    }
                })

        body = {
            'requests': resize_requests
        }

        service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()
        logging.info(f"Saved data to new tab '{tab_name}' in Google Sheet and added filters.")

    def update_google_sheet(self, data, tab_name, num_rows, num_cols):
        sheet = self.create_or_get_worksheet(tab_name, num_rows, num_cols)
        df = pd.DataFrame(data)

        # Add specific formatting for the "Papers" tab
        if tab_name == "Papers":
            self.format_papers_tab(sheet, df)
        else:
            self.format_worksheet(sheet, df, tab_name)

    def format_papers_tab(self, sheet, df):

        def convert_to_standard_date_format(date_str):
            try:
                # Try parsing the date with the format '%Y-%m-%d'
                return pd.to_datetime(date_str, format='%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                try:
                    # Try parsing the date with the format '%d %b %Y'
                    return pd.to_datetime(date_str, format='%d %b %Y').strftime('%Y-%m-%d')
                except ValueError:
                    # Return 'N/A' for dates that cannot be parsed
                    return date_str
                    # return 'N/A'

        # Apply the conversion function
        df['release_date'] = df['release_date'].apply(convert_to_standard_date_format)

        # Rename the columns to your desired names
        column_mapping = {
            'title': 'Title',
            'authors': 'Authors',
            'pdf_link': 'PDF link',
            'topics': 'Topics',
            'release_date': 'Release date',
            'referrer': 'Referrer'
        }
        df.rename(columns=column_mapping, inplace=True)

        # Transform the 'Referrer' column
        def create_hyperlink_formula(value):
            if pd.isna(value) or value == "":
                return "anon"
            elif "http" not in value:
                return value
            link_name = value.split("/")[-1]
            return f'=HYPERLINK("{value}", "{link_name}")'

        df['Referrer'] = df['Referrer'].apply(create_hyperlink_formula)

        # Clear the sheet before inserting new data
        sheet.clear()

        # Use gspread's `set_dataframe` to upload the whole DataFrame at once
        gspread_dataframe.set_with_dataframe(sheet, df, row=1, col=1, include_index=False, resize=True)

        # Set up filters, format header row in bold, and freeze the header using Google Sheets API
        service = build('sheets', 'v4', credentials=self.credentials)

        if df.shape[0] > 0:
            # Prepare the requests for bold formatting, center-align header, left-align content, and freezing header
            requests = [
                # Bold formatting request for header
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet.id,
                            'startRowIndex': 0,
                            'endRowIndex': 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True
                                },
                                'horizontalAlignment': 'CENTER'  # Center-align header text
                            }
                        },
                        'fields': 'userEnteredFormat.textFormat.bold, userEnteredFormat.horizontalAlignment'
                    }
                },
                # Left-align content rows
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet.id,
                            'startRowIndex': 1,  # Start from the row after header
                            'endRowIndex': df.shape[0] + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'LEFT'  # Left-align content
                            }
                        },
                        'fields': 'userEnteredFormat.horizontalAlignment'
                    }
                },
                # Freeze header row request
                {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet.id,
                            'gridProperties': {
                                'frozenRowCount': 1
                            }
                        },
                        'fields': 'gridProperties.frozenRowCount'
                    }
                },
                # Filter request
                {
                    'setBasicFilter': {
                        'filter': {
                            'range': {
                                'sheetId': sheet.id,
                                'startRowIndex': 0,
                                'endRowIndex': df.shape[0] + 1,
                                'startColumnIndex': 0,
                                'endColumnIndex': df.shape[1]
                            }
                        }
                    }
                }
            ]

            # Add request to rename the worksheet
            rename_sheet_request = {
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet.id,
                        'title': "Papers"
                    },
                    'fields': 'title'
                }
            }

            release_date_index = df.columns.get_loc(column_mapping['release_date'])

            date_format_request = {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet.id,
                        "startRowIndex": 1,
                        "endRowIndex": df.shape[0] + 1,
                        "startColumnIndex": release_date_index,
                        "endColumnIndex": release_date_index + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "yyyy-mm-dd"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
            # Sort request for 'Release date' column in descending order
            sort_request = {
                'sortRange': {
                    'range': {
                        'sheetId': sheet.id,
                        'startRowIndex': 1,  # Start from the row after header
                        'endRowIndex': df.shape[0] + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': df.shape[1]
                    },
                    'sortSpecs': [{
                        'dimensionIndex': release_date_index,
                        'sortOrder': 'DESCENDING'
                    }]
                }
            }

            # Append the sort request to the existing list
            requests.append(sort_request)

            requests.append(date_format_request)

            # Append the request to the existing list
            requests.append(rename_sheet_request)

            # Execute the requests
            body = {
                'requests': requests
            }

            service.spreadsheets().batchUpdate(spreadsheetId=os.getenv("GOOGLE_SHEET_ID"), body=body).execute()

        logging.info("Saved CSV data to Google Sheet, formatted header, and added filters.")

    def execute_requests(self, requests):
        body = {
            'requests': requests
        }
        service = build('sheets', 'v4', credentials=self.credentials)
        service.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()


def update_google_sheet(csv_file, tab_name, num_rows=1000, num_cols=None):
    updater = GoogleSheetUpdater(sheet_id=os.getenv("GOOGLE_SHEET_ID"), credentials_json=os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"))
    data = pd.read_csv(csv_file)

    if num_cols is None:
        num_cols = len(data.columns)

    updater.update_google_sheet(data=data, tab_name=tab_name, num_rows=num_rows, num_cols=num_cols)


def main():
    load_dotenv()
    repo_dir = root_directory()

    # Define the sheet update configurations
    sheets_to_update = [
        {"csv_file": f"{repo_dir}/data/paper_details.csv", "tab_name": "Papers", "num_cols": None},
        {"csv_file": f"{repo_dir}/data/links/websites.csv", "tab_name": "Website Links", "num_cols": None},
        {"csv_file": f"{repo_dir}/data/links/articles_updated.csv", "tab_name": "Articles", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/links/twitter_threads.csv", "tab_name": "Twitter Threads", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/links/youtube/recommended_youtube_videos_with_details.csv", "tab_name": "Recommended Youtube Videos", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/links/youtube/youtube_videos.csv", "tab_name": "Youtube Videos (from channel list)", "num_cols": 2},
    ]

    for config in sheets_to_update:
        update_google_sheet(config["csv_file"], config["tab_name"], num_cols=config["num_cols"])

    updater = GoogleSheetUpdater(sheet_id=os.getenv("GOOGLE_SHEET_ID"), credentials_json=os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"))
    # Update Google Sheet with YouTube handles
    youtube_txt_file = f"{repo_dir}/data/links/youtube/youtube_channel_handles.txt"
    youtube_data = {
        'YouTube Channel Handle': open(youtube_txt_file, 'r').read().split(','),
        'Link': [f'https://www.youtube.com/{handle.strip()}' for handle in open(youtube_txt_file, 'r').read().split(',')]
    }
    updater.update_google_sheet(data=youtube_data, tab_name="Podcasts & Youtube handles", num_rows=1000, num_cols=2)


if __name__ == "__main__":
    main()

