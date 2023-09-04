#!/bin/bash

# create virtual environment
python -m venv venv

# activate virtual environment
source venv/bin/activate

# install packages from requirements.txt
pip install -r requirements.txt

# create .env file with placeholder content
echo "GOOGLE_SHEET_CREDENTIALS_JSON=your_google_sheet_credentials_here" > .env

# deactivate virtual environment
deactivate