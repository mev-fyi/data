# Task backlog for project management purposes
Priority set: Low, Medium, High

## Contribution UX
- [ ] [Automate transcript downloads workflow as YouTube channels are added](#task-1)
    - [ ] [Fork youtube_video_and_transcript_downloader to this repo](#task-1)
    - Priority: Low
- [ ] [Optimise sorting of topics on Google Sheet](#task-1)
  - Priority: Low. Context: right now the topics are a single written to each cell. I believe the best UX would be a dropdown list which is populated for each cell such that a given user could select (ideally from a single column) all the tags of interest. 
- [ ] [Improve contribution UX for links beyond form](#task-1)
    - Priority: Low

## Codebase
### Devops
- [ ] [Create Google sheet staging environment](#task-2)
- Priority: Medium
- [ ] [Host site on Google Cloud](#task-2)
  - [ ] [Blocker: Streamlit LLM-app frontend](#task-2)
  - Priority: Low

### Back-end
####
- [ ] [Add support for Twitter threads](#task-2)
  - [ ] [Should they be written as blogs first?](#task-2)
  - Priority: High
- [ ] [Add support for blogs, articles](#task-2)
  - Priority: High
- [ ] [Add support for other research sites, journals](#task-2)
  - Priority: High
- [ ] [Automagically parse forms filled by contributors and append them to the relevant lists](#task-2)
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

