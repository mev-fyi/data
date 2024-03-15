from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import logging
import concurrent.futures

from src.populate_csv_files.get_article_content.ethglobal_hackathon.docs_parsers import site_configs, fetch_page, parse_content, save_page_as_pdf
# Assuming these are implemented elsewhere correctly

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def crawl_site(site_key, overwrite=False):
    config = site_configs.get(site_key)
    if not config:
        logging.error(f"No configuration found for site key: {site_key}")
        return

    # Retrieve the crawl function from the site config, falling back to the generic crawl function if not specified
    crawl_func = config.get('crawl_func', lambda config, overwrite=False: generic_crawl(config, overwrite))

    # Execute the crawl function
    crawl_func(config, overwrite)


def generic_crawl(config, overwrite):
    base_url = config['base_url']
    parser = config.get('parser', generic_parser)  # Get the parser from config, default to generic_parser

    current_url = base_url
    while current_url:
        content = fetch_page(current_url)
        if content:
            soup = BeautifulSoup(content, 'html.parser')
            parsed_data = parser(soup, config)
            save_page_as_pdf(parsed_data, current_url, overwrite, config.get('base_name', ''))

            # Logic for handling pagination (next button)
            next_button = soup.select_one(config.get('next_button_selector', '.pagination-nav__link--next'))
            if next_button and next_button.has_attr('href'):
                next_url = urljoin(current_url, next_button['href'])
                if next_url != current_url:
                    current_url = next_url
                    continue
        break


def update_img_src_with_absolute_urls(soup, base_url):
    for img in soup.find_all('img'):
        if img.has_attr('src'):
            img['src'] = urljoin(base_url, img['src'])


def generic_parser(soup, config):
    markdown_content = parse_content(soup, config['content_selector'], config['base_url'], config['img_selector'])
    return {'url': config['base_url'], 'content': markdown_content, 'author': "", 'date': ""}


def main(overwrite=False):
    docs = ['pimlico_permissionless', 'pimlico_bundler', 'pimlico_paymaster']
    # docs = None
    configs = {doc: site_configs[doc] for doc in docs} if docs is not None else site_configs

    # Define how many threads you want to use
    # Adjust this number based on your system and network capabilities
    max_workers = 18

    # Using ThreadPoolExecutor to run the crawling in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submitting all crawling jobs to the executor
        future_to_doc = {executor.submit(crawl_site, doc, overwrite): doc for doc in configs}

        # As each crawling job completes, log its completion
        for future in concurrent.futures.as_completed(future_to_doc):
            doc = future_to_doc[future]
            try:
                # The result of a future will be None since crawl_site does not return anything,
                # but this block can catch and log any exceptions raised during the crawl
                future.result()
                logging.info(f"Completed crawling for docs: [{doc}]")
            except Exception as exc:
                logging.error(f"{doc} generated an exception: {exc}")


if __name__ == '__main__':
    # Use a command line argument or environment variable to set overwrite if needed.
    overwrite = os.getenv('OVERWRITE_PDFS', 'False').lower() in ('true', '1')
    overwrite = True  # Forcing overwrite to True for this example, adjust as necessary
    main(overwrite)
