import pandas as pd
import re
from re import Pattern
from typing import Dict, List, Tuple
from abc import ABC, abstractmethod


from src.populate_csv_files.parse_new_data import url_patterns
from src.utils import root_directory

def find_longest_website_match_and_url(article_url, url_patterns):
    longest_match = ""
    longest_pattern = ""
    matched_url = ""
    for website_name, pattern in url_patterns.items():
        match = re.match(pattern, article_url)
        if match:
            if len(pattern) > len(longest_pattern):
                longest_match = website_name
                longest_pattern = pattern
                # Instead of extracting groups, use the entire matched string
                matched_url = article_url[:match.end()]

    if not matched_url:
        matched_url = "URL not defined for this website"

    # Include matched regex pattern in the return values
    return longest_match, matched_url, article_url, longest_pattern

def find_website_equivalent(regex_pattern, url_patterns):
    for website_name, pattern in url_patterns.items():
        if pattern == regex_pattern:
            return website_name
    return "Website equivalent not found"

def match_website_base(article_url, url_patterns):
    base_urls = set()  # Store unique base URLs
    for pattern in url_patterns.values():
        if re.match(pattern, article_url):
            try:
                base_pattern = re.sub(r"/[^/]+$", "/", pattern)
                base_match = re.match(base_pattern, article_url)
                if base_match:
                    base_urls.add(article_url[:base_match.end()])  # Add the base URL to the set
            except Exception as e:
                return f"Error: {e}"
    return ", ".join(base_urls)  # Join unique base URLs with comma separator

class Url:

    def __init__(self, value: str):
        self.value = value

    def __str__(self) -> str:
        return f"Url: {self.value}"
    
    def __contains__(self, match: str) -> bool:
        return match in self.value

class AuthorLink(Url):

    isAvailable = False

    def __init__(self, value: str = None):
        super().__init__(value)

    def is_available(self):
        return bool(self.value)

    def get_url(self):
        if not self.value:
            raise Exception("Author link not available")
        return self.value
    
    def __str__(self) -> str:
        if not self.value:
            return "Author Unknown"
        return super.__str__(self)

class ArticleMatcher(ABC):

    @abstractmethod
    def matches(self, url: Url) -> bool:
        pass

    @abstractmethod
    def get_author_link(self, url: Url) -> AuthorLink:
        pass

class MatchNotFoundException(Exception):
    
    def __init__(self, url: Url):
        super().__init__(f"Match not found for {url}.")

class AuthorUrlExtractor:
    article_matchers: List[ArticleMatcher]

    def __init__(self, article_matchers=[]):
        self.article_matchers: List[ArticleMatcher] = article_matchers

    def extract_url(self, url: Url) -> Url:
        match_count = sum(matcher.matches(url) for matcher in self.article_matchers)
        if match_count > 1:
            raise Exception(f"More than one matcher: ({match_count}) matched for {url}.")
        if match_count == 0:
            raise MatchNotFoundException(url)
        for matcher in self.article_matchers:
            if matcher.matches(url):
                return matcher.get_author_link(url)

        
class DeadEndsMatcher(ArticleMatcher):
    DEAD_ENDS = [
        "https://drive.google.com",
        "https://ethresear.ch",
        "www.iosco.org",
        "docs.google.com",
        "coinmarketcap.com",
        "rareskills.io",
        ]

    def matches(self, url: Url) -> bool:
        return sum( x in url for x in self.DEAD_ENDS) > 0
    
    def get_author_link(self, url: Url) -> AuthorLink:
        return AuthorLink()

class SlashCountKey:
    pattern: Pattern
    number: int
    def __init__(self, pattern, number):
        self.pattern = pattern
        self.number = number
    
    def matches(self, url: Url):
        return bool(re.search(self.pattern, url.value)) 

class SlashCountMatcher(ArticleMatcher):

    keys: List[SlashCountKey] = [
        SlashCountKey(r"galaxy.com/insights/research", 3),
        ] + [SlashCountKey(x, 1) for x in [
            r"notion.site",
            r"blog.metrika.co",
            r".medium.com",
            r".flashbots.net",
            r"vitalik.ca",
            r"frontier.tech",
            r"thedefiant.io",
            r"www.iexexchange.io",
            r"blog.uniswap.org",
            r"openreview.net",
            r"iex.io",
            r"paradigm.xyz",
            r"scrt.network",
            r"jumpcrypto.com",
            r"propellerheads.xyz",
            r"mechanism.org",
            r"a16zcrypto.com",
            r"research.lido.fi",
            r"research.anoma.net",
            r"osmosis.zone",
            r"insights.deribit.com",
            r"eips.ethereum.org",
            r".substack.com",
            r"ddmckinnon.com",
            r"hackingdistributed.com",
            r"bertcmiller.com",
            r"steakhouse.financial",
            r"ipfs.io",
            r"academic.oup.com",
            r"signalsandthreads.com",
            r"binance.com",
            r"umbraresearch.xyz",
            r"mangata.finance",
            r"dba.xyz",
            r".github.io",
            r"20squares.xyz",
            r"vitalik.eth.limo",
            r"monoceros.com",
            r"kelvinfichter.com",
            r"xenophonlabs.com",
            r"outlierventures.io",
            r"forum.celestia.org",
            r"research.arbitrum.io",
            r"gov.uniswap.org",
            r"dydx.forum",
            r"aave.com",
            r"arbitrum.foundation",
            r"aztec.network",
            r"conduit.xyz",
            r"helius.dev",
            r"gauntlet.xyz",
            r"mev.io",
            r"theblockchainerhub.xyz",
            r"duality.xyz",
            r"shutter.network",
            r"blocknative.com",
            r"openzeppelin.com",
            r"blog.qtum.org",
            r"merkle.io",
            r"chain.link",
            r"anoma.net",
            r"eip4844.com",
            r"clusters.xyz",
            r"cyfrin.io",
            r"quillaudits.com",
            r"nil.foundation",
            r"v4-by-example.org",
            r"cumberland.io",
            r"smlxl.io"
        ]] + [SlashCountKey(x, 2) for x in [
            r"hackmd.io",
            r"//mirror.xyz",
            r"notes.ethereum.org",
            r".mirror.xyz",
            r"/medium.com",
            r"github.com",
            r"paragraph.xyz",
            r"uniswapfoundation.org"
        ]]
    
    def matches(self, url: Url) -> bool:
        return sum(k.matches(url) for k in self.keys) > 0

    def get_author_link(self, url: Url) -> AuthorLink:
        k = next(k for k in self.keys if k.matches(url))
        backslash_suffix = '/'.join(("[^/]*" for _ in range(k.number)))
        return re.search(rf".*?//{backslash_suffix}", url.value).group()


class UrlPatternsMatcher(ArticleMatcher):

    MATCHERS = [
        ["test.test", r".*//mirror.xyz/[^/]*"]
    ]

    def matches(self, url: Url) -> bool:
        return sum(m[0] in url for m in self.MATCHERS) > 0

    def get_author_link(self, url: Url) -> AuthorLink:
        regex = next(m for m in self.MATCHERS if m[0] in url)[1]
        return re.search(regex, url.value).group()

def main():
    input_filepath = f"{root_directory()}/data/links/articles_updated.csv"
    articles = pd.read_csv(input_filepath)
    url_extractor = AuthorUrlExtractor([UrlPatternsMatcher(), DeadEndsMatcher(), SlashCountMatcher()])
    for i, url in enumerate(articles.article):
        print(url_extractor.extract_url(Url(url)))
    return 

if __name__ == "__main__":
    main()
