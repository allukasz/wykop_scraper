import pickle
import scraper as scrp
from scraper import save_to_pickle

def get_wykop_data(list_of_forums, current_year=2025, archives=False, archive_span=None):
    """
       Scrapes Wykop posts from multiple forums and optionally their archives. Due to how Wykop is set up comments are also treated as posts
       Data is incrementally saved to a pickle file after each forum/archive is processed, as a low-level backup option during long scraping sessions.

       Args:
           list_of_forums (list): List of Wykop URLs for targetted tags (e.g., ["https://wykop.pl/tag/forum/"])
           current_year (int, optional): Year to use for current scraping operations
           archives (bool, optional): Whether to also scrape archive pages.
           archive_span (tuple, optional): Year range for archive scraping as (start_year, end_year).
                                          Only used when archives=True. Defaults to None.

       Returns:
           pandas.DataFrame: Combined dataset containing all scraped posts, people, and temporal data
                            converted to a structured DataFrame format.

       Note:
           Uses auto_span=True for regular forum scraping, which automatically determines
           the appropriate time span for data collection.
       """

    master_list_posts = []
    master_list_people = []
    master_list_years = []

    for forum in list_of_forums:
        forum_page = f"{forum}strona/"
        print(f' new forum: {forum_page}')
        sub_result_posts, sub_result_people, sub_result_years = scrp.scrape_multiple(forum_page, 50, current_year, auto_span=True)
        master_list_posts.extend(sub_result_posts)
        master_list_people.extend(sub_result_people)
        master_list_years.extend(sub_result_years)
        save_to_pickle("pickle_container4.pickle", (master_list_people, master_list_posts, master_list_years))

        if archives is True:
            forum_link = f"{forum}archiwum/"
            print(f' new archive: {forum_link}')
            archive_posts_result, archive_people_result, archive_people_year = scrp.handle_archive(forum_link, archive_span)
            master_list_posts.extend(archive_posts_result)
            master_list_people.extend(archive_people_result)
            master_list_years.extend(archive_people_year)
            save_to_pickle("pickle_container4.pickle", (master_list_people, master_list_posts, master_list_years))

    final_dataset = scrp.convert_to_pandas(master_list_people, master_list_posts, master_list_years)

    #use the convert to pd function
    return final_dataset
