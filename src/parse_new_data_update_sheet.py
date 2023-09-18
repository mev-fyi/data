import populate_csv_files.parse_new_data
from src import get_research_paper_details, update_google_sheet
from src.populate_csv_files import pretty_print_articles

populate_csv_files.parse_new_data.run()
pretty_print_articles.run()
get_research_paper_details.main()
update_google_sheet.main()
