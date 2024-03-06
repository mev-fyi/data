import populate_csv_files.parse_new_data
from src import get_research_paper_details, update_google_sheet
from src.populate_csv_files import pretty_print_articles, fetch_youtube_video_details_from_handles, create_articles_thumbnails, create_research_paper_thumbnails, extract_recommended_youtube_video_name_from_link
from src.populate_csv_files.get_article_content import get_article_content, get_all_discourse_links, get_all_articles


populate_csv_files.parse_new_data.run()  # parse incoming data

fetch_youtube_video_details_from_handles.run()
# extract_recommended_youtube_video_name_from_link.run()

# pretty_print_articles.run()  # get article titles
# get_article_content.run(overwrite=True)  # get article content

# get_all_discourse_links.run()  # get discourse links
# get_all_articles.run(overwrite=False)  # get all articles from discourse

# get_research_paper_details.main()  # get research article details

# create_articles_thumbnails.main(headless=False)  # get article thumbnails
# create_research_paper_thumbnails.run()

update_google_sheet.main()
