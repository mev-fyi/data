import os
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import logging
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from src.utils import root_directory, parse_and_categorize_links


def update_google_sheet_with_websites(txt_file: str, sheet_id: str, new_tab_name: str) -> None:
    """
    Create a new tab in Google Sheet and populate it with websites from a TXT file.

    Parameters:
    - txt_file (str): Path to the TXT file.
    - sheet_id (str): ID of the Google Sheet to update.
    - new_tab_name (str): Name of the new tab to create.
    """
    # Set up credentials
    creds = Credentials.from_service_account_file(os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"), scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ])

    # Open the Google Sheet using gspread
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    # Check if the worksheet/tab exists
    try:
        sheet = spreadsheet.worksheet(new_tab_name)  # Try to get the worksheet by name
        logging.info(f"Worksheet '{new_tab_name}' already exists.")
    except gspread.WorksheetNotFound:  # This exception is raised if the worksheet does not exist
        # If the worksheet does not exist, create a new one
        sheet = spreadsheet.add_worksheet(title=new_tab_name, rows="1000", cols="1")
        logging.info(f"Created new worksheet: '{new_tab_name}'.")

    # Read the TXT file into a pandas DataFrame
    with open(txt_file, 'r') as file:
        websites = file.readlines()
    df = pd.DataFrame(websites, columns=['Websites'])

    # Use gspread's `set_dataframe` to upload the whole DataFrame at once
    set_with_dataframe(sheet, df, row=1, col=1, include_index=False, resize=True)

    # Set up filters for the column using Google Sheets API
    service = build('sheets', 'v4', credentials=creds)

    # Determine the range for the filter based on the shape of the DataFrame
    grid_range = {
        'sheetId': sheet.id,
        'startRowIndex': 0,
        'endRowIndex': df.shape[0] + 1,
        'startColumnIndex': 0,
        'endColumnIndex': df.shape[1]
    }

    # Set the filter using the setBasicFilter method
    body = {
        'requests': [{
            'setBasicFilter': {
                'filter': {
                    'range': grid_range
                }
            }
        }]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
    # Get the number of columns in the DataFrame
    num_columns = df.shape[1]

    # Auto-resize each column to fit the content
    resize_requests = []
    for col in range(num_columns):
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
    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

    logging.info("Saved TXT data to new tab in Google Sheet and added filters.")


def update_google_sheet_with_youtube_handles(txt_file: str, sheet_id: str, new_tab_name: str) -> None:
    """
    Create a new tab in Google Sheet and populate it with YouTube channel handles from a TXT file.

    Parameters:
    - txt_file (str): Path to the TXT file.
    - sheet_id (str): ID of the Google Sheet to update.
    - new_tab_name (str): Name of the new tab to create.
    """
    # Set up credentials
    creds = Credentials.from_service_account_file(os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"), scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ])

    # Open the Google Sheet using gspread
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    # Check if the worksheet/tab exists
    try:
        sheet = spreadsheet.worksheet(new_tab_name)  # Try to get the worksheet by name
        logging.info(f"Worksheet '{new_tab_name}' already exists.")
    except gspread.WorksheetNotFound:  # This exception is raised if the worksheet does not exist
        # If the worksheet does not exist, create a new one
        sheet = spreadsheet.add_worksheet(title=new_tab_name, rows="1000", cols="2")
        logging.info(f"Created new worksheet: '{new_tab_name}'.")

    # Read the TXT file into a list
    with open(txt_file, 'r') as file:
        handles = file.read().split(',')

    # Create DataFrame with names and links
    df = pd.DataFrame({
        'YouTube Channel Handle': handles,
        'Link': ['https://www.youtube.com/' + handle.strip() for handle in handles]
    })

    # Use gspread's `set_dataframe` to upload the whole DataFrame at once
    set_with_dataframe(sheet, df, row=1, col=1, include_index=False, resize=True)

    # Set up filters for the columns using Google Sheets API
    service = build('sheets', 'v4', credentials=creds)

    # Determine the range for the filter based on the shape of the DataFrame
    grid_range = {
        'sheetId': sheet.id,
        'startRowIndex': 0,
        'endRowIndex': df.shape[0] + 1,
        'startColumnIndex': 0,
        'endColumnIndex': df.shape[1]
    }

    # Set the filter using the setBasicFilter method
    body = {
        'requests': [{
            'setBasicFilter': {
                'filter': {
                    'range': grid_range
                }
            }
        }]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

    # Get the number of columns in the DataFrame
    num_columns = df.shape[1]

    # Auto-resize each column to fit the content
    resize_requests = []
    for col in range(num_columns):
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
    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
    logging.info("Saved YouTube handles to new tab in Google Sheet and added filters.")


def update_google_sheet_with_articles(csv_file: str, sheet_id: str, new_tab_name: str) -> None:
    """
    Create a new tab in Google Sheet and populate it with articles and referrers from a CSV file.

    Parameters:
    - csv_file (str): Path to the CSV file.
    - sheet_id (str): ID of the Google Sheet to update.
    - new_tab_name (str): Name of the new tab to create.
    """
    # Set up credentials
    creds = Credentials.from_service_account_file(os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"), scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ])

    # Open the Google Sheet using gspread
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    # Check if the worksheet/tab exists
    try:
        sheet = spreadsheet.worksheet(new_tab_name)  # Try to get the worksheet by name
        logging.info(f"Worksheet '{new_tab_name}' already exists.")
    except gspread.WorksheetNotFound:  # This exception is raised if the worksheet does not exist
        # If the worksheet does not exist, create a new one
        sheet = spreadsheet.add_worksheet(title=new_tab_name, rows="1000", cols="2")
        logging.info(f"Created new worksheet: '{new_tab_name}'.")

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Use gspread's `set_dataframe` to upload the whole DataFrame at once
    set_with_dataframe(sheet, df, row=1, col=1, include_index=False, resize=True)

    # Set up filters for the columns using Google Sheets API
    service = build('sheets', 'v4', credentials=creds)

    # Determine the range for the filter based on the shape of the DataFrame
    grid_range = {
        'sheetId': sheet.id,
        'startRowIndex': 0,
        'endRowIndex': df.shape[0] + 1,
        'startColumnIndex': 0,
        'endColumnIndex': df.shape[1]
    }

    # Set the filter using the setBasicFilter method
    body = {
        'requests': [{
            'setBasicFilter': {
                'filter': {
                    'range': grid_range
                }
            }
        }]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

    # Get the number of columns in the DataFrame
    num_columns = df.shape[1]

    # Auto-resize each column to fit the content
    resize_requests = []
    for col in range(num_columns):
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
    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

    logging.info("Saved articles data to new tab in Google Sheet and added filters.")


def update_google_sheet_with_twitter_threads(csv_file: str, sheet_id: str, new_tab_name: str) -> None:
    """
    Create a new tab in Google Sheet and populate it with twitter threads and referrers from a CSV file.

    Parameters:
    - csv_file (str): Path to the CSV file.
    - sheet_id (str): ID of the Google Sheet to update.
    - new_tab_name (str): Name of the new tab to create.
    """
    # Set up credentials
    creds = Credentials.from_service_account_file(os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"), scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ])

    # Open the Google Sheet using gspread
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    # Check if the worksheet/tab exists
    try:
        sheet = spreadsheet.worksheet(new_tab_name)  # Try to get the worksheet by name
        logging.info(f"Worksheet '{new_tab_name}' already exists.")
    except gspread.WorksheetNotFound:  # This exception is raised if the worksheet does not exist
        # If the worksheet does not exist, create a new one
        sheet = spreadsheet.add_worksheet(title=new_tab_name, rows="1000", cols="2")
        logging.info(f"Created new worksheet: '{new_tab_name}'.")

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Use gspread's `set_dataframe` to upload the whole DataFrame at once
    set_with_dataframe(sheet, df, row=1, col=1, include_index=False, resize=True)

    # Set up filters for the columns using Google Sheets API
    service = build('sheets', 'v4', credentials=creds)

    # Determine the range for the filter based on the shape of the DataFrame
    grid_range = {
        'sheetId': sheet.id,
        'startRowIndex': 0,
        'endRowIndex': df.shape[0] + 1,
        'startColumnIndex': 0,
        'endColumnIndex': df.shape[1]
    }

    # Set the filter using the setBasicFilter method
    body = {
        'requests': [{
            'setBasicFilter': {
                'filter': {
                    'range': grid_range
                }
            }
        }]
    }

    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

    # Get the number of columns in the DataFrame
    num_columns = df.shape[1]

    # Auto-resize each column to fit the content
    resize_requests = []
    for col in range(num_columns):
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
    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

    logging.info("Saved twitter threads data to new tab in Google Sheet and added filters.")


# Main execution
if __name__ == "__main__":
    repo_dir = root_directory()
    parse_and_categorize_links(input_filepath=os.path.join(repo_dir, "data/links/to_parse.csv"))
    update_google_sheet_with_websites(txt_file=os.path.join(repo_dir, "data/links/websites.txt"),
                                      sheet_id=os.getenv("GOOGLE_SHEET_ID"),
                                      new_tab_name="Website Links")

    update_google_sheet_with_youtube_handles(txt_file=os.path.join(repo_dir, "data/links/youtube_channel_handles.txt"),
                                             sheet_id=os.getenv("GOOGLE_SHEET_ID"),
                                             new_tab_name="Podcasts and Youtube videos")

    update_google_sheet_with_articles(csv_file=os.path.join(repo_dir, "data/links/articles.csv"),
                                      sheet_id=os.getenv("GOOGLE_SHEET_ID"),
                                      new_tab_name="Articles")

    update_google_sheet_with_twitter_threads(csv_file=os.path.join(repo_dir, "data/links/twitter_threads.csv"),
                                             sheet_id=os.getenv("GOOGLE_SHEET_ID"),
                                             new_tab_name="Twitter Threads")


