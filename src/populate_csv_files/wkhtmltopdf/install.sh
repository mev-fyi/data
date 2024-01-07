#!/bin/bash

# Update package lists
sudo apt-get update

# Install required dependencies
sudo apt-get install -y xfonts-75dpi wget

# Determine the Ubuntu version
UBUNTU_VERSION=$(lsb_release -sc)

# Determine the system architecture
ARCHITECTURE=$(dpkg --print-architecture)

# Define the base URL for the wkhtmltopdf releases
BASE_URL="https://github.com/wkhtmltopdf/packaging/releases/download"

# Map the Ubuntu version to the correct wkhtmltopdf version
WKHTMLTOX_URL=""
if [[ "$UBUNTU_VERSION" == "jammy" ]]; then
    WKHTMLTOX_URL="${BASE_URL}/0.12.6.1-2/wkhtmltox_0.12.6.1-2.jammy_${ARCHITECTURE}.deb"
elif [[ "$UBUNTU_VERSION" == "focal" ]]; then
    WKHTMLTOX_URL="${BASE_URL}/0.12.6-1/wkhtmltox_0.12.6-1.focal_${ARCHITECTURE}.deb"
elif [[ "$UBUNTU_VERSION" == "bionic" ]]; then
    WKHTMLTOX_URL="${BASE_URL}/0.12.6-1/wkhtmltox_0.12.6-1.bionic_${ARCHITECTURE}.deb"
elif [[ "$UBUNTU_VERSION" == "xenial" ]]; then
    WKHTMLTOX_URL="${BASE_URL}/0.12.6-1/wkhtmltox_0.12.6-1.xenial_${ARCHITECTURE}.deb"
else
    echo "Unsupported Ubuntu version: $UBUNTU_VERSION"
    exit 1
fi

# Check if the URL is reachable before attempting to download
if ! wget --spider "$WKHTMLTOX_URL"; then
    echo "The wkhtmltopdf package could not be found at the URL: $WKHTMLTOX_URL"
    echo "Please check the URL and ensure it is correct."
    exit 1
fi

# Download the wkhtmltopdf package
echo "Downloading wkhtmltopdf from: $WKHTMLTOX_URL"
wget -q "$WKHTMLTOX_URL" -O wkhtmltox.deb || { echo "Download failed"; exit 1; }

# Install the downloaded package
sudo dpkg -i wkhtmltox.deb || { echo "Installation failed"; exit 1; }

# If there are any missing dependencies after the initial dpkg command, fix them
sudo apt-get -f install -y || { echo "Failed to fix dependencies"; exit 1; }

# Clean up: Remove the downloaded .deb file
rm wkhtmltox.deb

# Confirm installation
if wkhtmltopdf --version; then
    echo "wkhtmltopdf installed successfully."
else
    echo "Failed to confirm wkhtmltopdf installation."
    exit 1
fi
