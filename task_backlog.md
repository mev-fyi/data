# Task backlog for project management purposes
Priority set: Low, Medium, High
Complexity set: Low, Medium, High

## Contributions
###  UX
- [ ] [Optimise sorting of topics on Google Sheet](#task-1)
  - Priority: Low. Context: right now the topics are a single written to each cell. I believe the best UX would be a dropdown list which is populated for each cell such that a given user could select (ideally from a single column) all the tags of interest. 
- [ ] [Optimise sorting of authors on Google Sheet](#task-1)
  - Priority: Low. Context: right now the authors are a single written to each cell. I believe the best UX would be a dropdown list which is populated for each cell such that a given user could select (ideally from a single column) all the authors of interest.
- [ ] [Improve contribution UX for links beyond form](#task-1)
    - Priority: Low
- [ ] [Create Twitter bot to contribute either a whole thread when called, else the links in the tweet content](#task-1)
    - Priority: Low
- [ ] [Add a safe zotero parser and filter for MEV contributions](#task-1)
    - Priority: Medium
- [ ] [Run routine / event-based script on Typeform Results content to be uploaded](#task-1)
    - Priority: Medium
- [ ] [Create new URL patterns with LLM call given existing URL base](#task-1)
    - Priority: Medium
    - Context: adding url patterns is tedious and low added value. It would be great to have a script that can generate the URL patterns given a base URL and knowledge base of previous URL patterns.
### Content  
- [ ] [Add the content from bookmarks dotted all over](#task-1)
    - Priority: High
- [ ] [Create a community-led curation mechanism?](#task-1)
    - Priority: Medium
    - Context: It would be powerful for community to flag paper complexity and create a "MEV onboarding roadmap" from beginner to advanced. In the best case scenario, there would be a large amount of inbound content. We would hopefully need to curate its complexity sooner or later.

## Research exploitation UX
- [ ] [Scrape all websites for new uploads and add links to uploads in most recent order on a single worksheet](#task-1)
  - Priority: Medium
- [ ] [Add a podcast tab sorted by most recent of content from the podcasts handle worksheet](#task-1)
    - Priority: Medium
    - Context: Right now besides going into each Podcast radio, there is no way to see the most recent content. This would be a powerful way to keep up to date with the latest content.
- [ ] [Add Twitter bot to Tweet for each new scrapped research document](#task-1)
  - Priority: Low

- [ ] [add GPT-generated tags to each link](#task-1)
  - Priority: Low 

## mev.fyi governance
- [ ] [Deprecate typeform for scalable open-source solution with links waiting to be uploaded](#task-1)
- [ ] [Decentralise domain name ownership](#task-1)
- [ ] [Decentralise google sheet ownership](#task-1)
  - Context: for now the google sheet is updated by a single person via a google service account which is quite centralised. Technically if the .env were pointed to another sheet that can be replicated or become the new source of truth (would require a new url forwarding which is easy to do). 
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
- [ ] [Consider moving from Google Sheets to self-hosted website for analytics purposes (and UX?)](#task-4)
  - Priority: Medium
  - Context: it is hard to gauge how successful the project is without analytics. This is a trade-off between privacy (would rely on cookies) and analytics. Such analytics could help possibly (if approved/validated/not rejected) monetise the project e.g. by adding a job tabs / sponsored content and re-invest proceeds into the development of the project. Such analytics would help on the business development part.
