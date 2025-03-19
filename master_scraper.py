import pickle
import scraper as scrp
from scraper import save_to_pickle

def get_wykop_data(list_of_forums, current_year=2025, archives=False, archive_span=None):

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

if __name__ == "__main__":
    # andrzej, jandrzej, candrzej = get_wykop_data(["https://wykop.pl/tag/przegryw/", "https://wykop.pl/tag/blackpill/", "https://wykop.pl/tag/pieklomezczyzn/"], True, (2023,2024))
    # print(len(andrzej), len(jandrzej), len(candrzej))
    # print(andrzej, jandrzej)
    #
    # end_product = get_wykop_data(["https://wykop.pl/tag/logikarozowychpaskow/", "https://wykop.pl/tag/samotnosc/"], archives=True, archive_span=(2020,2025))
    # end_product.to_csv('final_continued_witharchive.csv')
    # #tba "https://wykop.pl/tag/stulejacontent/" --> scrape this one without archives
    with open ('pickle_container5.pickle', 'rb') as fp:
        list_1 = pickle.load(fp)
    # #
    # pupa = scrp.convert_to_pandas(list_1[0], list_1[1], list_1[2])
    # pupa.to_csv('niebieskiepaski.csv')
    print(list_1[1])
    print(len(list_1[0]), len(list_1[1]), len(list_1[2]))

    # koniec = scrp.scrape_multiple("https://wykop.pl/tag/stulejacontent/strona/", 372, 2025, auto_span=False )
    # save_to_pickle("pickle_container5.pickle",( koniec[0], koniec[1], koniec[2]))
    # tabela = scrp.convert_to_pandas(koniec[1], koniec[0], koniec[2])
    # tabela.to_csv("stulejacontent_noarchive.csv")