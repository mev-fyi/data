import populate_csv_files.parse_new_data
from src import get_research_paper_details, update_google_sheet
from src.populate_csv_files import pretty_print_articles, fetch_youtube_video_details_from_handles, create_articles_thumbnails, create_research_paper_thumbnails, extract_recommended_youtube_video_name_from_link
from src.populate_csv_files.get_article_content import get_article_content, get_all_discourse_links, get_all_articles, get_docs
from src.populate_csv_files.get_article_content.ethglobal_hackathon import scrap_docs
from src.populate_csv_files.get_article_content.get_docs import process_repositories

only_run_yt=False

# populate_csv_files.parse_new_data.run()  # parse incoming data
# #
# fetch_youtube_video_details_from_handles.run()
# extract_recommended_youtube_video_name_from_link.run()

if only_run_yt == False:
    pretty_print_articles.run()  # get article titles
    get_article_content.run(overwrite=False)  # get article content from handpicked articles
    # #
    # get_docs.process_repositories()  # get docs for Ethereum org, flashbots, suave
    # #
    # scrap_docs.main(overwrite=False)

    get_all_discourse_links.run()  # get discourse links
    get_all_articles.run(overwrite=False)  # get all articles from discourse
    #
    get_research_paper_details.main()  # get research article details
    #
    create_articles_thumbnails.main(headless=False)  # get article thumbnails
    create_research_paper_thumbnails.run()

update_google_sheet.main()
