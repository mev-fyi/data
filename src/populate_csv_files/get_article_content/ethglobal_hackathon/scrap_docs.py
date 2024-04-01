from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import logging
import concurrent.futures

from src.populate_csv_files.get_article_content.ethglobal_hackathon.docs_parsers import fetch_page, parse_content, save_page_as_pdf, update_or_append_csv, clean_csv_titles
from src.populate_csv_files.get_article_content.ethglobal_hackathon.site_configs import site_configs
from src.utils import root_directory

# Assuming these are implemented elsewhere correctly
from threading import Lock

# Initialize a lock for CSV file operations
csv_file_lock = Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

visited_urls = set()  # Add this at the beginning of the script to track visited URLs globally

def crawl_site(site_key, csv_lock, overwrite_docs=False, headless_browser=True):
    config = site_configs.get(site_key)
    if not config:
        logging.error(f"No configuration found for site key: {site_key}")
        return

    # Define a wrapper function that correctly passes the lock argument without duplication
    def crawl_func_wrapper(config, overwrite_docs=overwrite_docs, headless_browser=headless_browser):
        if 'crawl_func' in config:
            # Call the specific crawl function from the config
            config['crawl_func'](config, overwrite=overwrite_docs, lock=csv_lock, headless=headless_browser, visited_urls=visited_urls)
        else:
            # Fallback to a generic crawl function if none is specified
            generic_crawl(config, overwrite_docs, lock=csv_lock, headless=headless_browser)

    # Now call the wrapper function, which handles passing the lock correctly
    crawl_func_wrapper(config)



def generic_crawl(config, overwrite, lock, headless=False):
    global visited_urls
    try:
        base_url = config['base_url']
        parser = config.get('parser', generic_parser)

        current_url = base_url
        while current_url:
            if current_url in visited_urls:
                break  # Skip this URL if it has already been visited
            visited_urls.add(current_url)  # Mark this URL as visited

            content = fetch_page(current_url)
            if content:
                soup = BeautifulSoup(content, 'html.parser')
                parsed_data = parser(soup, config)
                pdf_path = save_page_as_pdf(parsed_data, current_url, overwrite, config.get('base_name', ''))
                if pdf_path:
                    csv_path = os.path.join(root_directory(), "data", "docs_details.csv")
                    update_or_append_csv(pdf_path, current_url, parsed_data['content'], csv_path, overwrite, lock)

                next_button = soup.select_one(config.get('next_button_selector', '.pagination-nav__link--next'))
                if next_button and next_button.has_attr('href'):
                    next_url = urljoin(current_url, next_button['href'])
                    if next_url != current_url:
                        current_url = next_url
                        continue
            break
    except Exception as e:
        logging.error(f"Error while crawling {config['base_url']}: {e}")


def update_img_src_with_absolute_urls(soup, base_url):
    for img in soup.find_all('img'):
        if img.has_attr('src'):
            img['src'] = urljoin(base_url, img['src'])


def generic_parser(soup, config):
    markdown_content = parse_content(soup, config['content_selector'], config['base_url'], config['img_selector'])
    return {'url': config['base_url'], 'content': markdown_content, 'author': "", 'date': ""}


def main(docs=None, overwrite=False, max_workers=18, headless=True):
    # docs = None
    configs = {doc: site_configs[doc] for doc in docs} if docs is not None else site_configs

    # Define how many threads you want to use
    # Adjust this number based on your system and network capabilities

    # Using ThreadPoolExecutor to run the crawling in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submitting all crawling jobs to the executor
        future_to_doc = {executor.submit(crawl_site, doc, csv_file_lock, overwrite, headless): doc for doc in configs}

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
    clean_csv_titles()
    overwrite = os.getenv('OVERWRITE_PDFS', 'False').lower() in ('true', '1')

    docs = ['brink']
    # docs = None
    main(docs=docs, overwrite=False, headless=False, max_workers=15)
