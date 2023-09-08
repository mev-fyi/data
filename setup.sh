#!/bin/bash

# create virtual environment
python -m venv venv

# activate virtual environment
source venv/bin/activate

# install packages from requirements.txt
pip install -r requirements.txt

# create .env file with placeholder content
echo "GOOGLE_SHEET_CREDENTIALS_JSON=your_google_sheet_credentials_here" > .env
echo "GOOGLE_SHEET_ID==the_google_sheet_id_here" > .env
echo "YOUTUBE_DATA_API_KEY=the_youtube_data_api_key_here" > .env
echo "FETCH_NEW_PDF=False" > .env

# deactivate virtual environment
deactivate