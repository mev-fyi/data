import populate_csv_files.parse_new_data
from src import get_research_paper_details, update_google_sheet
from src.populate_csv_files import pretty_print_articles, fetch_youtube_video_details_from_handles, create_articles_thumbnails
from src.populate_csv_files.get_article_content import get_article_content

# fetch_youtube_video_details_from_handles.run()
populate_csv_files.parse_new_data.run()  # parse incoming data
# pretty_print_articles.run()  # get article titles
# get_article_content.run(overwrite=True)  # get article content

# create_articles_thumbnails.main()  # get article thumbnails

# get_research_paper_details.main()  # get research article details
# update_google_sheet.main()
