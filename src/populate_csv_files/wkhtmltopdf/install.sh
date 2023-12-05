#!/bin/bash

# Update package lists
sudo apt-get update

# Install required dependencies
sudo apt-get install -y xfonts-75dpi

# Download the wkhtmltopdf package
wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-2/wkhtmltox_0.12.6.1-2.jammy_amd64.deb

# Install the downloaded package
sudo dpkg -i wkhtmltox_0.12.6.1-2.jammy_amd64.deb

# Clean up: Remove the downloaded .deb file (optional)
rm wkhtmltox_0.12.6.1-2.jammy_amd64.deb

# Confirm installation
wkhtmltopdf --version
