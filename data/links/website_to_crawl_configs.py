sites_config = {
    "Mechanism": {
        "base_url": "https://www.mechanism.org/",
        "table_page_url": "https://www.mechanism.org/",
        "table_wrapper_selector": ".table-wrapper",
        "row_selector_template": "a.table-row",
    },
    "Frontier": {
        "base_url": "https://frontier.tech/",
        "table_page_url": "https://frontier.tech/",
        "table_wrapper_selector": "#block-05455506b5104fc08f4982f3a8948fea",
        "row_selector_template": "a",
        "exclude_links": [
            "mailto:research@frontier.tech",
            "https://twitter.com/FrontierDotTech",
            "open-positions"
        ]
    },
    "MediumPublication_Alcueca": {
        "base_url": "https://alcueca.medium.com/",
        "table_page_url": "https://alcueca.medium.com/",
        "table_wrapper_selector": "div.ae:nth-child(2)",
        "row_selector_template": "article a",
        "exclude_links": ['https://medium.com/tag/blockchain?source=user_profile--------------------blockchain-----------------'],
        "use_selenium": True,
    }
}
