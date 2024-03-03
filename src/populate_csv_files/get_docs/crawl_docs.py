import csv

from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from src.utils import return_driver, root_directory

def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_all_website_links_selenium(url, driver):
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    urls = set()
    domain_name = urlparse(url).netloc
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            continue
        full_url = urljoin(url, href.split('#')[0])  # Strip fragment.
        if not is_valid_url(full_url):
            continue
        if domain_name in full_url:
            urls.add(full_url)
    return urls


def save_urls(urls, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['URL']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for url in urls:
            writer.writerow({'URL': url})


def crawl_website_selenium(start_url):
    driver = return_driver()
    visited_urls = set()
    urls_to_visit = get_all_website_links_selenium(start_url, driver)
    while urls_to_visit:
        current_url = urls_to_visit.pop()
        if current_url in visited_urls:
            continue
        print("Visiting:", current_url)
        visited_urls.add(current_url)
        urls_to_visit = urls_to_visit.union(get_all_website_links_selenium(current_url, driver))
    driver.quit()
    return visited_urls


def crawl_websites_in_parallel(start_urls):
    all_visited_urls = {}  # Change to a dict to hold urls for each start_url
    with ThreadPoolExecutor(max_workers=len(start_urls)) as executor:
        future_to_url = {executor.submit(crawl_website_selenium, url): url for url in start_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                if url in all_visited_urls:
                    all_visited_urls[url].update(data)
                else:
                    all_visited_urls[url] = data
                print(f"Completed crawling {url}")
            except Exception as exc:
                print(f"{url} generated an exception: {exc}")

    # Save the URLs to CSV files, one per start_url
    data_dir = os.path.join(root_directory(), "data", "docs")
    os.makedirs(data_dir, exist_ok=True)
    for url, urls in all_visited_urls.items():
        base_name = urlparse(url).netloc.replace('.', '_') + urlparse(url).path.replace('/', '_').rstrip('_')
        save_urls(urls, os.path.join(data_dir, f"{base_name}.csv"))


if __name__ == "__main__":
    start_urls = ["https://community.optimism.io/docs/governance/",
                  "https://docs.uniswap.org/",
                  "https://docs.aave.com/hub/",
                  "https://docs.compound.finance/",
                  "https://docs.balancer.fi/",
                  "https://docs.chain.link/",
                  "https://doc.rust-lang.org/book/",
                  "https://docs.1inch.io/",
                  "https://docs.ambient.finance/",
                  "https://gmxio.gitbook.io/gmx/",
                  "https://solana.com/docs",
                  "docs.avax.network",
                  "https://docs.polygon.technology/",
                  "https://internetcomputer.org/docs/current/home",
                  "https://docs.near.org/",
                  "https://docs.cosmos.network/",
                  "https://docs.stacks.co/",
                  "https://aptos.dev/",
                  "https://docs.optimism.io/",
                  "https://community.optimism.io/docs/biz/",
                  "https://community.optimism.io/docs/contribute/",
                  "https://docs.bittensor.com/",
                  "https://docs.lido.fi/",
                  "https://docs.mantle.xyz/network/introduction/overview",
                  "https://know.rendernetwork.com/documentation/rndr-user-manuel",
                  "https://docs.celestia.org/",
                  "https://docs.arbitrum.io/welcome/get-started",
                  "https://docs.sei.io/",
                  "https://docs.arweave.org/developers/mining/mining-guide",
                  "https://developer.algorand.org/docs/",
                  "https://docs.makerdao.com/smart-contract-modules/mkr-module",
                  "https://docs.sui.io/",
                  "https://docs.ordinals.com/",
                  "https://docs.aave.com/hub/",
                  "https://docs.starknet.io/documentation/",
                  "https://book.starknet.io/",
                  "https://docs.cairo-lang.org/",
                  "https://docs.starknet.io/documentation/tools/devtools/",
                  "https://docs.pyth.network/home",
                  "https://docs.worldcoin.org/",
                  "https://docs.dydx.exchange/",
                  "https://docs.blur.foundation/",
                  "https://docs.dymension.xyz/",
                  "https://docs.blast.io/about-blast",
                  "https://docs.osmosis.zone/",
                  "https://docs.jup.ag/",
                  "https://docs.osmosis.zone/",
                  "https://resources.curve.fi/",
                  "https://docs.ens.domains/",
                  "https://docs.oceanprotocol.com/",
                  "https://dev.zilliqa.com/",
                  "https://docs.across.to/introduction/quickstart-guide",
                  "https://docs.across.to/v/user-docs/",
                  "https://dev.zilliqa.com/",
                  "https://docs.eigenlayer.xyz/eigenlayer/overview",
                  "https://www.jito.network/docs/jitosol/overview/"
                  "https://docs.wormhole.com/wormhole/",
                  "https://www.quicknode.com/docs/welcome",
                  "https://docs.orca.so/"
                  "https://docs.marinade.finance/",
                  "https://docs.opensea.io/",
                  "https://docs.mango.markets/",
                  "https://docs.magiceden.io/",
                  "https://docs.altlayer.io/altlayer-documentation/welcome/overview",
                  "https://docs.sushi.com/"]
    crawl_websites_in_parallel(start_urls)
