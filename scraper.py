import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import pickle as pckl
import time


#custom get function for URLS; delaying the connection with server in case of temporary timeouts
def custom_get(url):
    try:
        result = requests.get(url)
        return  result
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        result = retry_get(url)
        return result

#delay getting a URL
def retry_get(url, retries=100, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            return response
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"Attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    return None

#function to scrape a page; collects url's of inidivudal posts and feeds them to the decode function; returns list of posts and authors
def scrape(URL):

    page = custom_get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    #get all "a" tags with post-like attributes
    posty = soup.find_all('a', attrs={
        'class': '',
        'data-v-3dac138d': True,
        'href': lambda href: href and href.startswith('/wpis/')
    })
    #extract all the links
    list_of_links = [post["href"] for post in posty]

    post, author = decode_posts(list_of_links)

    return post, author

#function that takes a list of links, and takes out their authors and links
def decode_posts(list_of_links):

    text_list = []
    author_list = []

    for post in list_of_links:

        url = "https://wykop.pl" + post

        page = custom_get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        script_tags = soup.find_all('script', type='application/ld+json')
        text, author = handle_json(script_tags)

        if text not in text_list:
            text_list.append(text)
            author_list.append(author)

        #deleted posts have a different signature, make sure they are still read; put a dummy text
        for deleted in soup.select("section.entry.reply section.alert p"):
            fake_comment = soup.new_tag("section", **{"class": "entry-content"})
            fake_comment.string = "[Komentarz usunięty]"
            deleted.replace_with(fake_comment)

        comments = soup.select("section.entry.reply section.entry-content")
        authors = soup.select("section.entry.reply span[data-v-173bf079]")

        new_texts = [comment.get_text(strip=True, separator=" ") for comment in comments]
        new_people = [author.get_text(strip=True, separator=" ") for author in authors]

        #make sure that only pages where all posts and authorship contributions were scraped properly are added - needed for pandas later on
        if len(new_texts) == len(new_people):
            text_list.extend(new_texts)
            author_list.extend(new_people)

    return text_list, author_list


#this function written with help of genAI; post content is within a JSON
def handle_json(a_json):
    for item in a_json:
        try:
            data = json.loads(item.string)

            if isinstance(data, list):
                for entry in data:
                    if "articleBody" in entry:
                        name = entry.get("author", {}).get("name", "Unknown")
                        return entry["articleBody"], name
            elif "articleBody" in data:
                name = data.get("author", {}).get("name", "Unknown")  # Fixed variable name
                return data["articleBody"], name
        except (json.JSONDecodeError, AttributeError) as e:
            print("JSON Decode Error:", e)

    print("missing data in Json")  # Debugging
    return "-", "Unknown"

#function that takes a forum url, amount of pages to be scraped, year and can spontaneously generare page span if needed
def scrape_multiple(url, page_count, year, auto_span=False):

    end_list_posts = []
    end_list_people = []

    #get number of pages in a forum
    if auto_span is True:
        page = custom_get(url + '1')
        soup = BeautifulSoup(page.content, "html.parser")
        res = soup.find('li',attrs={'class':'paging last'})
        pages = re.search(r'>(\d+)<', str(res))

       #error handling since some archive pages have an oddly named paging counter that does not work; if no counter scrape only the current page
        try:
            page_count = int(pages.group(1))
        except AttributeError:
            page_count = 100

    #scrape every page
    for iteration in range(1, page_count+1):
        URL = url + str(iteration)
        post, person = scrape(URL)
        end_list_posts.extend(post)
        end_list_people.extend(person)
        print(f"page {iteration} finished; elements = {len(post), len(person)}; page = {URL}")

    #make a list of post's year matching the authors and post lists in length; necessary for pandas
    a_year = int(year)
    year_list = [a_year] * len(end_list_people)

    return end_list_posts, end_list_people, year_list

#function that handles archives, takes a forum and a span of years during which the archive should be searched; returns posts, authors and years of posts
def handle_archive(url, year_span):

    archival_posts = []
    archival_people = []
    archival_years = []

    for current_year in range(year_span[0], year_span[1]):
        url_year = url + str(current_year)
        urls = supply_months_to_archive(url_year)
        for link in urls:
            #print(link)
            try: #make sure that archive exists
                page = custom_get(link)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                continue
            else:
                posts, people, years = scrape_multiple(link, 50, current_year, auto_span=True)
                archival_posts.extend(posts)
                archival_people.extend(people)
                archival_years.extend(years)

    return archival_posts, archival_people, archival_years

#function to dynamically create a list of months to archival years
def supply_months_to_archive(year):
    monthly_links = []
    month_count = range(1, 13)
    for intg in month_count:
        if len(str(intg)) == 1:
            link = f"{year}-0{intg}/strona/"
        else:
            link = f"{year}-{intg}/strona/"
        monthly_links.append(link)
    return monthly_links

def convert_to_pandas(ppl_list, post_list, year_list):
    data_dict= {"user": ppl_list, "post_content": post_list, "post_year": year_list}
    wykop_dataset = pd.DataFrame(data=data_dict)
    return wykop_dataset

def save_to_pickle(file_name, objects):

    with open (file_name, 'wb') as fp:
        pckl.dump(objects, fp)
    return None

if __name__ == "__main__":
    pass
    # soup, moup, doup= scrape_multiple("https://wykop.pl/tag/przegryw/archiwum/2018-06/strona/", 2, 2025, auto_span=False)
    # print(soup, moup)
    # print (len(soup), len(moup))

    #tests

    #singular page
    # page = requests.get('https://wykop.pl/wpis/79939761/przegryw-wreszcie-gram-w-gta-iv-to-byla-kiedys-moj')
    # soup2 = BeautifulSoup(page.content, "html.parser")
    # # print(soup2.prettify())
    #
    # posty = soup2.find_all('a', attrs={
    #         'class': '',
    #         'data-v-3dac138d': True,
    #         'href': lambda href: href and href.startswith('/wpis/')
    #     })
    #
    # list_of_links = [post["href"] for post in posty]
    # print(list_of_links)



    # #specific post, check comments
    # dupa, fiut = decode_posts(['/wpis/33256763/przegryw-stulejacontent-tfwnogf-normiki-maja-udane#SocialMediaPosting'])
    # print(dupa, fiut)
    # print(len(dupa), len(fiut))

    #archive generation w/ months
    # lista = supply_months_to_archive("https://wykop.pl/tag/przegryw/archiwum/2018")
    # print(lista)

    #archive scrapping
    # soup, moup, doup = handle_archive('https://wykop.pl/tag/przegryw/archiwum/', (1999,2001))
    # print(doup)
    # print (len(soup), len(moup))
