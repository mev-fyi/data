import logging
import os

import gspread
import gspread_dataframe
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


def update_google_sheet_with_csv(csv_file: str, sheet_id: str) -> None:
    """
    Update a Google Sheet with the contents of a CSV file.

    Parameters:
    - csv_file (str): Path to the CSV file.
    - sheet_id (str): ID of the Google Sheet to update.
    """
    # Set up credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv("GOOGLE_SHEET_CREDENTIALS_JSON"), [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ])

    # Load CSV data into a pandas DataFrame
    df = pd.read_csv(csv_file)

    def convert_to_standard_date_format(date_str):
        try:
            # First, try the default format
            return pd.to_datetime(date_str, format='%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            # If it fails, try the "dd MMM yyyy" format
            return pd.to_datetime(date_str, format='%d %b %Y').strftime('%Y-%m-%d')

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

    # Open the Google Sheet using gspread
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1

    # Clear the sheet before inserting new data
    sheet.clear()

    # Use gspread's `set_dataframe` to upload the whole DataFrame at once
    gspread_dataframe.set_with_dataframe(sheet, df, row=1, col=1, include_index=False, resize=True)

    # Set up filters, format header row in bold, and freeze the header using Google Sheets API
    service = build('sheets', 'v4', credentials=creds)

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

    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()

    logging.info("Saved CSV data to Google Sheet, formatted header, and added filters.")
