# Task backlog for project management purposes
Priority set: Low, Medium, High

## Contributions
###  UX
- [ ] [Automate transcript downloads workflow as YouTube channels are added](#task-1)
    - [ ] [Fork youtube_video_and_transcript_downloader to this repo](#task-1)
    - Priority: Low
- [ ] [Optimise sorting of topics on Google Sheet](#task-1)
  - Priority: Low. Context: right now the topics are a single written to each cell. I believe the best UX would be a dropdown list which is populated for each cell such that a given user could select (ideally from a single column) all the tags of interest. 
- [ ] [Improve contribution UX for links beyond form](#task-1)
    - Priority: Low
- [ ] [Create Twitter bot to contribute either a whole thread when called, else the links in the tweet content](#task-1)
    - Priority: Low
- [ ] [Add a safe zotero parser and filter for MEV contributions](#task-1)
    - Priority: Medium
- [ ] [Run routine / event-based script on Typeform Results content to be uploaded](#task-1)
    - Priority: Medium
### Content  
- [ ] [Add the content from bookmarks dotted all over](#task-1)
    - Priority: High
- [ ] [Create a community-led curation mechanism?](#task-1)
    - Priority: Medium
    - Context: It would be powerful for community to flag paper complexity and create a "MEV onboarding roadmap" from beginner to advanced. In the best case scenario, there would be a large amount of inbound content. We would hopefully need to curate its complexity sooner or later.


## Research exploitation UX
- [ ] [Scrape all websites for new uploads and add links to uploads in most recent order on a single worksheet](#task-1)
  - Priority: Medium
- [ ] [Add a youtube video & podcast tab sorted by most recent of content from the Youtube channels/podcasts](#task-1)
    - Priority: Medium
    - Context: Right now besides going into each Youtube/Podcast channel, there is no way to see the most recent content. This would be a powerful way to keep up to date with the latest content.
- [ ] [Add Twitter bot to Tweet for each new scrapped research document](#task-1)
  - Priority: Low

- [ ] [add GPT-generated tags to each link](#task-1)
  - Priority: Low 

## mev.fyi governance
- [ ] [Deprecate typeform for scalable open-source solution with links waiting to be uploaded](#task-1)
- [ ] [Decentralise domain name ownership](#task-1)
- [ ] [Decentralise google sheet ownership](#task-1)
- [ ] [Decentralise github repo ownership](#task-1)

## Codebase
### Devops
- [ ] [Create Google sheet staging environment](#task-2)
  - Priority: High :) 
- [ ] [Host site on Google Cloud](#task-2)
  - [ ] [Blocker: Streamlit LLM-app frontend](#task-2)
  - Priority: Low

### Back-end
####
- [ ] [Add support for Twitter threads](#task-2)
  - [ ] [Should they be written as blogs first?](#task-2)
  - Priority: High
- [ ] [Continue adding support for other research sites and journals](#task-2)
  - Priority: High

- [ ] [Check https://apify.com/apify/website-content-crowler](#task-2)
  - Priority: Low

#### LLM chatbot and inference
- [ ] [Design architecture for LLM-based chatbot for papers, blogs, podcast recommendation (backend)](#task-2)
  - Priority: Medium
  - Context: There is a trade-off between using off-the-shelf solution (IBM Watson Discovery for summarizing + service client like Intercom) vs. building from scratch with Langchain + vector database and HuggingFace or GPT backend. Latter likely more scalable with open-source contributions.
- [ ] [Add LLM-based chatbot for recommending papers, blogs, podcast recommendation (backend)](#task-3)
  - Priority: Medium
- [ ] [Generate MEV glossary, definitions based on Youtube transcripts, podcasts, papers](#task-6)
  - Priority: Low
- [ ] [Improve transcript generation e.g. with LLM inference (AssemblyAI) on .mp3 youtube video content](#task-5)
  - Priority: Low

### Frontend
- [ ] [Add Streamlit frontend for LLM-based chatbot for papers, blogs, podcast recommendation](#task-4)

