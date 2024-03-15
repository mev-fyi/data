import requests
from bs4 import BeautifulSoup
import csv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def resolve_url(short_url):
    """
    Resolve the shortened URL to its final destination.
    """
    # Use a session to reuse the TCP connection
    with requests.Session() as session:
        try:
            # HEAD request to avoid downloading the content
            response = session.head(short_url, allow_redirects=True, timeout=10)
            return response.url
        except requests.RequestException as e:
            logging.error("Failed to resolve URL %s: %s", short_url, e)
            return short_url  # Return the original URL if unable to resolve


def scrape_prizes():
    logging.info("Starting scrape...")

    # URL of the ETHGlobal London 2024 prizes page
    url = 'https://ethglobal.com/events/london2024/prizes'

    # Send a GET request
    response = requests.get(url)
    if response.status_code != 200:
        logging.error("Failed to fetch the webpage. Status code: %s", response.status_code)
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # Container for the scraped data
    data = []

    # Find all prize giver divs by looking for divs with an id and a specific class indicating a border.
    for prize_giver in soup.find_all('div', id=True, class_='border-b-2'):
        prize_giver_id = prize_giver.get('id')
        # Find all links within each prize giver's section.
        links = prize_giver.select('div.grid.grid-cols-2.gap-2 a')
        for link in links:
            if link.has_attr('href'):
                # Resolve the URL to its final destination
                resolved_url = resolve_url(link['href'])
                data.append([prize_giver_id, resolved_url])
                logging.info("Added URL for %s: %s", prize_giver_id, resolved_url)

    # Write the data to a CSV file
    with open('prizes_links.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Prize Giver', 'URL'])
        writer.writerows(data)

    logging.info("Scraping completed and data written to prizes_links.csv")


def main():
    scrape_prizes()


if __name__ == "__main__":
    main()
