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
from concurrent.futures import ThreadPoolExecutor

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

        if 'Release date' in df.columns:
            # Sort DataFrame in descending order by 'Release date' if the tab is 'Articles'
            df['Release date'] = pd.to_datetime(df['Release date'], errors='coerce')
            df.sort_values('Release date', ascending=False, inplace=True, na_position='last')
            # Convert 'Release date' back to string in 'yyyy-mm-dd' format
            df['Release date'] = df['Release date'].dt.strftime('%Y-%m-%d')

        # Check if 'in DB' is part of the tab name to perform specific operations
        if 'in DB' in tab_name:
            # Replace underscores with spaces in column names
            df.columns = df.columns.str.replace('_', ' ')
            # Check for a 'Release Date' column and sort if exists
            date_col_name = next((col for col in df.columns if 'release date' in col.lower()), None)
            if date_col_name:
                # Convert 'Release Date' to datetime, sort, and then back to string if necessary
                df[date_col_name] = pd.to_datetime(df[date_col_name], errors='coerce')
                df.sort_values(by=date_col_name, ascending=False, inplace=True)
                df[date_col_name] = df[date_col_name].dt.strftime('%Y-%m-%d')

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
            # Combine patterns for "publish(ed) date" and "releas(ed) date" into one
            pattern = re.compile(r'(publish(ed)?|releas(ed)?)[_\s]?date', re.IGNORECASE)

            # Iterate through the column names to find a match
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
        # Identify columns that should not be auto-resized fully
        # For example, excluding "Pdf link" and "Authors"
        excluded_columns = ['Pdf link', 'Authors']
        # Convert DataFrame column names to the format used in the sheet
        df_columns = [col[0].upper() + col[1:] for col in df.columns]

        resize_requests = []
        for i, col_name in enumerate(df_columns):
            if col_name.lower() in ['title', 'release date', 'website', 'channel_name', 'channel name', 'twitter thread', 'youtube channel handle', 'release_date']:
                # AutoResize only "Title" and "Release Date" columns
                resize_requests.append({
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1
                        }
                    }
                })
            elif col_name not in excluded_columns:
                # For other columns, optionally set a default width instead of auto-resizing
                # This section is optional and can be adjusted or removed based on your requirements
                resize_requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1
                        },
                        "properties": {
                            "pixelSize": 100  # Set your desired default width here
                        },
                        "fields": "pixelSize"
                    }
                })

        # Now, add these resize requests to your list of requests
        # Assuming `requests` is your list of other formatting requests
        requests += resize_requests

        # Execute all requests in a batchUpdate call
        body = {"requests": requests}
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


def update_youtube_data(repo_dir):
    updater = GoogleSheetUpdater(sheet_id=os.getenv("GOOGLE_SHEET_ID"), credentials_json=os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"))
    youtube_txt_file = f"{repo_dir}/data/links/youtube/youtube_channel_handles.txt"
    youtube_data = {
        'YouTube Channel Handle': open(youtube_txt_file, 'r').read().split(','),
        'Link': [f'https://www.youtube.com/{handle.strip()}' for handle in open(youtube_txt_file, 'r').read().split(',')]
    }
    updater.update_google_sheet(data=youtube_data, tab_name="YT channel handles", num_rows=1000, num_cols=2)


def main():
    load_dotenv()
    repo_dir = root_directory()

    rag_path_to_db = f"{repo_dir}/../rag/pipeline_storage/"

    # Define the sheet update configurations
    sheets_to_update = [
        {"csv_file": f"{repo_dir}/data/paper_details.csv", "tab_name": "Papers", "num_cols": None},
        {"csv_file": f"{repo_dir}/data/links/websites.csv", "tab_name": "Websites", "num_cols": None},
        {"csv_file": f"{repo_dir}/data/links/articles_updated.csv", "tab_name": "Articles", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/links/twitter_threads.csv", "tab_name": "Threads", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/links/youtube/recommended_youtube_videos_with_details.csv", "tab_name": "YT videos (recommended)", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/links/youtube/youtube_videos.csv", "tab_name": "YT videos (channel list)", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/docs_details.csv", "tab_name": "Docs", "num_cols": 2},
        {"csv_file": f"{repo_dir}/data/links/merged_articles.csv", "tab_name": "All Discourse articles", "num_cols": 2},

        {"csv_file": f"{rag_path_to_db}research_papers.csv", "tab_name": "Research papers in DB", "num_cols": 2},
        {"csv_file": f"{rag_path_to_db}articles.csv", "tab_name": "Articles in DB", "num_cols": 2},
        {"csv_file": f"{rag_path_to_db}docs.csv", "tab_name": "Docs in DB", "num_cols": 2},
        {"csv_file": f"{rag_path_to_db}youtube_videos.csv", "tab_name": "Videos in DB", "num_cols": 2},
        {"csv_file": f"{rag_path_to_db}all_discourse_articles.csv", "tab_name": "Discourse Articles in DB", "num_cols": 2},
    ]

    # Using ThreadPoolExecutor to parallelize the updates
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(update_google_sheet, config["csv_file"], config["tab_name"], config["num_cols"]) for
                   config in sheets_to_update]

        # Wait for all futures to complete
        for future in futures:
            future.result()

    update_youtube_data(repo_dir)


if __name__ == "__main__":
    main()
