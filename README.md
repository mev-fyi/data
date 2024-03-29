### (1) View: [data.mev.fyi](https://data.mev.fyi), (2) Contribute: [add.mev.fyi](https://add.mev.fyi), (3) site: [mev.fyi](https://mev.fyi)

# MEV.FYI is the MEV research content aggregator and search engine

Welcome to `mev.fyi` - the open-source initiative dedicated to gathering and curating research on Maximal Extractable Value (MEV), incentive alignment, mechanism design, and their implications in the blockchain ecosystem, with a specific focus on Ethereum.

`mev.fyi`'s end-game is to become the go-to resource for MEV research and related topics using a chatbot interface to recommend papers, articles, and videos based on user input (based on [nfx.com/chat](https://www.nfx.com/chat)).
This repository can be the basis to create guides and formal definitions of MEV and the relevant Ethereum research as a whole, for education purposes e.g. to formalise and agree on definitions, onboard new (re)searchers [...].

## What is MEV?

Maximal Extractable Value (MEV) refers to the total amount that can be extracted from blockchain users by strategically ordering, including, or censoring transactions. It's an emerging area of study and concern in the blockchain space.

## Project Goals

1. **Research Aggregation**: Gather the latest and most relevant academic papers, articles, and discussions surrounding MEV and related topics.
2. **Community Contributions**: Encourage and integrate contributions from the community to ensure the repository remains updated and relevant.
3. **Education**: Educate the broader blockchain and Ethereum community about the implications of MEV and how it can shape the future of decentralized platforms.

## Where to start the MEV learning journey?
1. Flashbots' MEV [Research repository](https://github.com/flashbots/mev-research/) is a great place to start. It contains a curated list of academic papers, articles, and discussions on MEV and related topics.

2. Beyond Flashbots, all articles available from sources in resources.md constitute strong foundations to get started on MEV research.

3. Research papers, can be browsed on the Google Sheet [here](https://docs.google.com/spreadsheets/d/1POtuj3DtF3A-uwm4MtKvwNYtnl_PW6DPUYj6x7yJUIs/edit#gid=1299175463). This file contains a list of research papers, the papers' title, authors, topics, and link.

## YouTube Video Transcripts

We've curated a selection of YouTube channels focused on MEV and related topics. Each channel's videos come with their respective transcripts to enable easy "Ctrl+F" searches. This repository offers an invaluable tool to parse video content with LLMs, create glossaries, definitions, and more.

Disclaimer: each transcript is fetched from Youtube API and automatically generated. The Google algorithm generating said scripts is imperfect and some words can be misinterpreted.  

## Topics of Interest
Flashbots' topics and definitions list [here](https://github.com/flashbots/mev-research/blob/main/topics.md) enumerates topics of interest to the MEV research community. These topics include:
- Auction Design
- Cryptographic Privacy
- Cryptoeconomic Privacy
- MEV in L2
- MEV Taxonomy
- Account Abstraction
- Protocol Design
- Search Optimization

And, more broadly:
- Incentive Alignment in Blockchain Systems
- Mechanism Design and its Implications in Ethereum
- Transaction Ordering and its Impacts
- Auction theory and its Applications in Blockchain
- Potential Solutions and Mitigations for MEV

## How to Contribute with Research

We welcome contributions from everyone, irrespective of your background or expertise level!
You can contribute either using the form here [add.mev.fyi](https://add.mev.fyi) or using via GitHub as per the instructions below:

1. **Fork the Repository**: Start by forking this repository.
2. **Clone & Create a New Branch**: Clone the repository to your local machine and create a new branch.
3. **Make Changes**: Add your contributions namely 
   1. link to a relevant research paper in data/links/arxiv_papers.csv alongside your handle if you want to be noted as paper referrer. 
   2. Articles, discussion thread, forum post or any educational content can be added to data/links/articles.csv alongside your handle if you want to be noted as article referrer.
4. **Push & Create a Pull Request**: Push your changes to your fork and then open a pull request against this repository.

## How to Contribute to the tools around Research
There is maintained work item list [here](task_backlog.md) that you can pick up and work on. If you want to add a new item, please open an issue and we will discuss it.

## Support the Initiative

Love what we're doing? Star the repository and spread the word! Your support helps in keeping this initiative active and growing.

# How to setup the environment ?

```
./setup.sh
source venv/bin/activate
export PYTHONPATH=$(pwd)
python src/populate_csv_files/get_article_content/get_websites_from_articles.py
```

---

© 2023 mev.fyi | Open-sourced under the [MIT License](LICENSE).