import logging
import csv
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from src.utils import return_driver_get_discourse, root_directory
from concurrent.futures import ThreadPoolExecutor

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_existing_links(csv_name):
    csv_path = f'{root_directory()}/data/links/articles/{csv_name}.csv'
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return [row['Link'] for row in reader]
    except FileNotFoundError:
        logging.info(f"No existing CSV found for {csv_name}. Starting fresh.")
        return []


def convert_date(date_str):
    try:
        # First, try parsing the date assuming format 'Jul '23'
        return datetime.strptime(date_str, "%b '%y").strftime('%Y-%m-%d')
    except ValueError:
        try:
            # Next, try parsing with format 'Jan 29', assuming the current year
            current_year = datetime.now().year
            return datetime.strptime(f"{date_str} {current_year}", "%b %d %Y").strftime('%Y-%m-%d')
        except ValueError:
            try:
                # Then, try parsing with format 'May 2021'
                return datetime.strptime(date_str, "%b %Y").strftime('%Y-%m-%d')
            except ValueError:
                # If it fails, check for relative dates like '1h' or '2d'
                if date_str.endswith('h') or date_str.endswith('d'):
                    number = int(date_str[:-1])
                    unit = date_str[-1]
                    current_time = datetime.now()
                    if unit == 'h':
                        return (current_time - timedelta(hours=number)).strftime('%Y-%m-%d')
                    elif unit == 'd':
                        return (current_time - timedelta(days=number)).strftime('%Y-%m-%d')
                # Log an error if none of the formats match
                print(f"Error parsing date: {date_str}")
                return None


def scrape_forum_links(base_url, csv_name):
    logging.info(f"Starting to scrape {base_url} for links...")
    existing_links = load_existing_links(csv_name)

    try:
        driver = return_driver_get_discourse()
        driver.get(base_url)
    except Exception as e:
        logging.error(f"Error getting driver for {base_url}: {e}")
        return


    def scroll_to_bottom():
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.25)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            # logging.info("Scrolled to current bottom of the page.")

    scroll_to_bottom()

    data = []
    topics = driver.find_elements(By.CSS_SELECTOR, "tr.topic-list-item")
    for idx, topic in enumerate(topics, start=1):
        try:
            link_element = topic.find_element(By.CSS_SELECTOR, "td.main-link.clearfix.topic-list-data > span > a")
            link = link_element.get_attribute('href')
            title = link_element.text

            if link in existing_links:
                logging.info("Found link already in CSV. Stopping.")
                break

            if 'dydx' in csv_name:
                date_element = topic.find_element(By.CSS_SELECTOR, "td:nth-child(4) > div:nth-child(2) > a:nth-child(3) > span:nth-child(1)")
            else:
                date_element = topic.find_element(By.CSS_SELECTOR, "td.num.topic-list-data.age.activity > a > span")
            date_text = date_element.text
            date = convert_date(date_text)

            if 'dydx' in csv_name:
                author_element = topic.find_element(By.CSS_SELECTOR, "td:nth-child(4) > div:nth-child(2) > span:nth-child(1) > a:nth-child(1)")
            else:
                author_element = topic.find_element(By.CSS_SELECTOR, "td.posters.topic-list-data > a:nth-child(1)")
            author_link = author_element.get_attribute('href')

            # logging.info(f"[{csv_name}] Processing topic [{idx}]: [{title}]")
            data.append([link, author_link, date, title])
        except Exception as e:
            logging.error(f"[{csv_name}] Error processing topic {idx}: {e}")

    # Updated CSV writing part with newline parameter
    csv_path = f'{root_directory()}/data/links/articles/{csv_name}.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Link', 'Author', 'Release Date', 'Title'])
        for row in data:
            writer.writerow(row)

    logging.info(f"Total {len(data)} new records have been extracted and saved to CSV for {csv_name}.")
    logging.info("Scraping completed for " + base_url)

    driver.quit()


def run():
    forums = [
        ("https://collective.flashbots.net/c/research/20", "flashbots_research"),
        ("https://collective.flashbots.net/c/ship/18", "flashbots_ship"),
        ("https://collective.flashbots.net/c/relays/15", "flashbots_relays"),
        ("https://collective.flashbots.net/c/builders/14", "flashbots_builders"),
        ("https://collective.flashbots.net/c/searchers/12", "flashbots_searchers"),
        ("https://collective.flashbots.net/c/data/13", "flashbots_data"),
        ("https://collective.flashbots.net/c/frp/24", "flashbots_frp"),
        ("https://collective.flashbots.net/c/suave/27", "flashbots_suave"),
        ("https://ethresear.ch/", "ethresearch"),
        ('https://research.lido.fi/', "lido_research"),
        ('https://research.anoma.net/c/anoma-research-topics/34', "anoma_research_topics"),
        ('https://research.arbitrum.io', "arbitrum_research"),
        ('https://gov.uniswap.org', "uniswap_governance"),
        ('https://governance.aave.com/c/governance/4', "aave_governance"),
        ('https://governance.aave.com/c/risk/7', "aave_risk"),
        ('https://governance.aave.com/c/development/26', "aave_development"),
        ("https://governance.aave.com/c/learning-center/21", "aave_learning_center"),
        ("https://research.anoma.net/c/anoma-research-topics/34", "anoma_research_topics"),
        ("https://research.anoma.net/c/rfc-anoma/29", "anoma_rfc"),
        ("https://research.anoma.net/c/protocol-design/25", "anoma_protocol_design"),
        ("https://research.anoma.net/c/education/20", "anoma_education"),
        ("https://research.anoma.net/c/comparative-architecture/19", "anoma_comparative_architecture"),
        ("https://research.anoma.net/c/new-age-economics/26", "anoma_new_age_economics"),
        ("https://research.anoma.net/c/self-sovereign-social/27", "anoma_self_sovereign_social"),
        ("https://research.anoma.net/c/cryptography-zoo/28", "anoma_cryptography_zoo"),
        ("https://research.anoma.net/c/programming-language-theory/31", "anoma_programming_language_theory"),
        ("https://research.anoma.net/c/solver-shenanigans/30", "anoma_solver_shenanigans"),
        ("https://research.anoma.net/c/general/4", "anoma_general"),
        ('https://forum.celestia.org/c/research/5', "celestia_research"),
        ('https://forum.celestia.org/c/developers/16', "celestia_developers"),
        ("https://forum.celestia.org/c/node/13", "celestia_node"),
        ("https://forum.celestia.org/c/celestia-improvement-proposal-cip/31", "celestia_cip"),
        ("https://forum.celestia.org/c/community/23", "celestia_community"),
        ("https://forum.celestia.org/c/ecosystem/22", "celestia_ecosystem"),
        ('https://dydx.forum', "dydx_forum"),
        ('https://forum.arbitrum.foundation/c/proposals/7', "arbitrum_proposals"),
        ('https://forum.arbitrum.foundation/c/dao-grant-programs/16/', "arbitrum_dao_grant_programs"),
        ('https://forum.arbitrum.foundation/c/grants-discussions/11/', "arbitrum_grants_discussions"),
        ('https://forum.arbitrum.foundation/c/governance/6/', "arbitrum_governance"),
        ('https://forum.arbitrum.foundation/c/general/4', "arbitrum_general"),
        ('https://forum.arbitrum.foundation/c/discussion-and-thoughtful-commentary-related-to-arbitrum-products-such-as-arbitrum-one-arbitrum-nova/5', "arbitrum_commentary"),
        ('https://forum.arbitrum.foundation/c/arbitrum-govhack/26', "arbitrum_govhack"),
        ('https://forum.aztec.network/c/aztec/5', "aztec_network"),
        ('https://forum.aztec.network/c/general/4', "aztec_general"),
        ('https://forum.aztec.network/c/noir/7', 'aztec_noir'),
        ('https://gov.optimism.io/c/proposals/38', "optimism_proposals"),
        ('https://gov.optimism.io/c/retropgf/46', "optimism_retropgf"),
        ('https://gov.optimism.io/c/governance/41', "optimism_governance"),
        ('https://gov.optimism.io/c/citizens-house-gov/79', "optimism_citizens_house_gov"),
        ('https://gov.optimism.io/c/monitoring/62', "optimism_monitoring"),
        ("https://forum.eigenlayer.xyz/c/protocol/7", "eigenlayer_protocol"),
        ("https://forum.eigenlayer.xyz/c/new-lst-token-on-eigenlayer/21", "eigenlayer_new_lst_token"),
        ("https://forum.eigenlayer.xyz/c/eigenda/9", "eigenlayer_eigenda"),
        ("https://forum.eigenlayer.xyz/c/middleware/8", "eigenlayer_middleware"),
        ("https://forum.eigenlayer.xyz/c/general/4", "eigenlayer_general"),
        ("https://forum.makerdao.com/c/sakura-subdao/88", "makerdao_sakura_subdao"),
        ("https://forum.makerdao.com/c/spark-subdao/84", "makerdao_spark_subdao"),
        ("https://forum.makerdao.com/c/quantitative-subdao/90", "makerdao_quantitative_subdao"),
        ("https://forum.makerdao.com/c/qualitative-subdao/86", "makerdao_qualitative_subdao"),
        ("https://forum.makerdao.com/c/maker-core/92", "makerdao_maker_core"),
        ("https://forum.makerdao.com/c/proposal-ideas/94", "makerdao_proposal_ideas"),
        ("https://forum.makerdao.com/c/developers-corner/93", "makerdao_developers_corner"),
        ("https://forum.makerdao.com/c/avcs/98", "makerdao_avcs"),
        ("https://forum.makerdao.com/c/alignment-conserver/78", "makerdao_alignment_conserver"),
        ("https://forum.makerdao.com/c/legacy/74", "makerdao_legacy"),
        ("https://forum.scrt.network/c/general-faq/9", "scrt_network_general_faq"),
        ("https://forum.scrt.network/c/secret-network/46", "scrt_network_secret_network"),
        ("https://forum.scrt.network/c/secret-contracts/50", "scrt_network_secret_contracts"),
        ("https://forum.scrt.network/c/governance/47", "scrt_network_governance"),
        ("https://forum.scrt.network/c/secret-nodes/8", "scrt_network_secret_nodes"),
        ("https://forum.scrt.network/c/developer-help/10", "scrt_network_developer_help"),
        ("https://forum.scrt.network/c/private-computation/6", "scrt_network_private_computation"),
        ("https://forum.scrt.network/c/uncategorized/1", "scrt_network_uncategorized"),
        ("https://community.taiko.xyz/", "taiko_community"),
        ("https://forum.numer.ai", "numerai_forum"),
        ("https://forum.apecoin.com/c/announcements/17", "apecoin_announcements"),
        ("https://forum.apecoin.com/c/general/14", "apecoin_general"),
        ("https://forum.apecoin.com/c/aip-execution-and-transparency/62", "apecoin_aip_execution_and_transparency"),
        ("https://forum.apecoin.com/c/aip-ideas/20", "apecoin_aip_ideas"),
        ("https://forum.apecoin.com/c/aip-drafts/21", "apecoin_aip_drafts"),
        ("https://forum.apecoin.com/c/administrative-review/36", "apecoin_administrative_review"),
        ("https://forum.apecoin.com/c/live-aip/13", "apecoin_live_aip"),
        ("https://forum.apecoin.com/c/final-aips/22", "apecoin_final_aips"),
        ("https://forum.apecoin.com/c/help-resources/26", "apecoin_help_resources"),
        ("https://forum.apecoin.com/c/withdrawn/18", "apecoin_withdrawn"),
        ("https://jupresear.ch", "jupiter_exchange_research"),
        ("https://forums.mev.io/c/announcements/5", "mev_announcements"),
        ("https://forums.mev.io/c/general/4", "mev_general"),
        ("https://forums.manifoldfinance.com/c/changelog/8", "manifold_finance_changelog"),
        ("https://forums.manifoldfinance.com/c/docs/43", "manifold_finance_docs"),
        ("https://forums.manifoldfinance.com/c/research/5", "manifold_finance_research"),
        ("https://forums.manifoldfinance.com/c/thepit/9", "manifold_finance_thepit"),
        ("https://forums.manifoldfinance.com/c/development/6", "manifold_finance_development"),
        ("https://forums.manifoldfinance.com/c/governance/7", "manifold_finance_governance"),
        ("https://forum.dymension.xyz/c/multidimensional-governance-forum/9", "dymension_multidimensional_governance_forum"),
        ("https://forum.dymension.xyz/c/research-development/6", "dymension_research_development"),
        ("https://forum.dymension.xyz/c/community/7", "dymension_community"),
        ("https://forum.dymension.xyz/c/rollapps/8", "dymension_rollapps"),
        ("https://forum.cow.fi/c/general/5", "cow_fi_general"),
        ("https://forum.cow.fi/c/cow-improvement-proposals-cip/6/none", "cow_fi_cip"),
        ("https://forum.cow.fi/c/tech-spells/2", "cow_fi_tech_spells"),
        ("https://forum.cow.fi/c/cow-grants-program/10/none", "cow_fi_grants_program"),
        ("https://forum.cow.fi/c/knowledge-base/8", "cow_fi_knowledge_base"),
        ("https://forum.cow.fi/c/treasury-overview-of-cow-dao/21", "cow_fi_treasury_overview"),
    ]
    # TODO 2024-03-01: automatically extract all subforums from the parent forum page
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(scrape_forum_links, forum[0], forum[1]) for forum in forums]
        for future in futures:
            future.result()  # Wait for each task to complete


if __name__ == "__main__":
    run()
