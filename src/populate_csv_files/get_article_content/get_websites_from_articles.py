import pandas as pd
import re
from re import Pattern
from typing import Dict, List
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

class ArticleMatcher(ABC):

    @abstractmethod
    def matches(self, url: Url) -> bool:
        pass

    @abstractmethod
    def get_author_link(self, url: Url) -> Url:
        pass

class MatchNotFoundException(Exception):
    pass

class AuthorUrlExtractor:
    article_matchers: List[ArticleMatcher]

    def __init__(self, website_matchers):
        self.article_matchers: List[ArticleMatcher] = website_matchers

    def extract_url(self, url: Url) -> Url:
        match_count = sum(matcher.matches(url) for matcher in self.article_matchers)
        if match_count > 1:
            raise Exception(f"More than one matcher: ({match_count}) matched for url: {url}")
        if match_count == 0:
            raise MatchNotFoundException()
        for matcher in self.article_matchers:
            if matcher.matches(url):
                return matcher.get_author_link(url)
        

class UrlPatternsMatcher(ArticleMatcher):

    def __init__(self, url_patterns: Dict[str, str]):
        self.url_patterns = url_patterns
        self.matchers: Dict[Pattern[str]] = dict()

    def matches(self, url: Url) -> bool:
        return   


def main():
    input_filepath = f"{root_directory()}/data/links/articles_updated.csv"
    articles = pd.read_csv(input_filepath)
    for url in articles.article:
        print(url)
    return 
    # Iterate through each article URL, find its website match, and get the equivalent URL
    for _, row in articles_df.iterrows():
        try:
            matched_website, matched_url, article_url, matched_pattern = find_longest_website_match_and_url(row['article'], url_patterns)
            website_equivalent = find_website_equivalent(matched_pattern, url_patterns)
            website_base = match_website_base(article_url, url_patterns)
            # print('')
            print_statement = f"Article URL: {article_url}\nMatched Website: {matched_website}\nMatched Website URL: {matched_url}\nMatched Regexp: {matched_pattern}\nWebsite Equivalent: {website_equivalent}\nWebsite Base: {website_base}\n{article_url[:article_url.rfind('/')+1]}"
            if matched_website == "Website not found" or website_base == "Website base not found" or "Error" in website_base:
                print(f"Article URL: {article_url}")
                if matched_website == "Website not found":
                    print(f"Matched Website: {matched_website}")
                    print(print_statement)
                if website_base == "Website base not found":
                    print(f"Website Base: {website_base}")
                    print(print_statement)
                if "Error" in website_base:
                    print(f"Error: {website_base.split(': ')[1]}")
            else:
                # print(f"Article URL: {article_url}\nMatched Website: {matched_website}\nMatched Website URL: {matched_url}\nMatched Regexp: {matched_pattern}\nWebsite Equivalent: {website_equivalent}\nWebsite Base: {website_base}\n{article_url[:article_url.rfind('/')+1]}")
                pass
        except re.error as e:
            print(f"Error in regex pattern matching: {e}")

if __name__ == "__main__":
    main()
