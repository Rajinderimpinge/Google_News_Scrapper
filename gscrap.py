"""
Script to retrieve the data from google News with some keywords and date filters
"""
import feedparser
from pandas.io.json import json_normalize
from datetime import datetime
import pandas
import urllib.parse
import requests, lxml
import re
import pycountry
from bs4 import BeautifulSoup
import schedule
import time
import argparse


class GoogleScapper:
    """
    This class deals with Google news scrapper
    """

    def __init__(self, *args, **kwargs):
        self.keywords = kwargs.get("keywords", None)
        self.start_date = kwargs.get("start_date", None)
        self.end_date = kwargs.get("end_date", None)
        self.hl = kwargs.get("location", None)
        self.gl = kwargs.get("region", None)
        self.base_url = "https://news.google.com/rss/"

        self.columns = [
            "title",
            "link",
            "id",
            "guidislink",
            "published",
            "summary",
            "source.href",
            "source.title",
        ]
        self.headers = {
            'accept': '*/*',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36 Edg/85.0.564.44'
        }

    def __validate_date__(self, date):
        """
        Validate dates format required for the filters
        return: True if date is valid else Flase
        """
        try:
            date_format = datetime.strptime(date, "%Y-%m-%d")
        except:
            return False

        return True

    def validate_inputes(self, keywords, start_date, end_date):
        """
        Validate all Inputes like keywods, Start date and end date
        return: True if all the inputs dare correct
        """
        if keywords is None or keywords == "":
            print("Please provide keyword.")
            return False

        if start_date and end_date:
            start_date = self.__validate_date__(start_date)
            end_date = self.__validate_date__(end_date)

            if not start_date or not end_date:
                print("Please use 'YYYY-MM-DD' date format")
                return False
        return True

    def start(self):
        """
        This function is initial point for scrapping
        """
        if self.validate_inputes(self.keywords, self.start_date, self.end_date):
            return self.__start__()

    def __start__(self):
        """
        This function return data after processing the keywords
        """
        return self.__get_url__(keywords=self.keywords)

    def get_google_news_feed(self):
        """
        Google News feed
        """
        return self.__get_url__(keywords=self.keywords)

    def get_article(self, card):
        """Extract article information from the raw html"""
        title = card.find('h4', 's-title').text
        source = card.find("span", 's-source').text
        posted = card.find('span', 's-time').text.replace('·', '').strip()
        description = card.find('p', 's-desc').text.strip()
        raw_link = card.find('a').get('href')
        unquoted_link = requests.utils.unquote(raw_link)
        pattern = re.compile(r'RU=(.+)\/RK')
        clean_link = re.search(pattern, unquoted_link).group(1)

        article = (title, source, posted, clean_link)
        return article, description

    def get_yahoo_feed(self):
        """Run the main program"""
        df_columns = ['title', 'source', 'posted', 'link']

        df = pandas.DataFrame(columns=df_columns)
        articles = []
        descriptions = list()
        for page_no in range(1, 20, 10):
            url = f'https://news.search.yahoo.com/search?p={self.keywords}&b={page_no}'
            links = set()
            print(url)
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            cards = soup.find_all('div', 'NewsArticle')
            for card in cards:
                article, description = self.get_article(card)
                link = article[-1]
                if not link in links:
                    links.add(link)
                    articles.append(article) 
                descriptions.append(description)
        df = pandas.DataFrame(articles, columns=df_columns)
        df = df.assign(description=descriptions)
        return df

    def get_bing_article(self, card):
        title = card.select_one('.title').text
        link = card.select_one('.title')['href']
        snippet = card.select_one('.snippet').text
        source = card.select_one('.source a').text
        date_posted = card.select_one('#algocore span+ span').text
        article = (title, source, date_posted, link)
        return article, snippet

    def get_bing_feed(self):
        articles = []
        # df_columns = ['title', 'Source', 'Posted', 'snippet', 'Link']
        df_columns = ['title', 'source', 'posted', 'link']
        url = f'https://www.bing.com/news/search?q={self.keywords}'
        print(url)
        snippets = list()
        Description = list()
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        for card_data in soup.select('.card-with-cluster'):
            article, snippet = self.get_bing_article(card_data)
            articles.append(article)
            snippets.append(snippet)
        df = pandas.DataFrame(articles, columns=df_columns)
        df = df.assign(snippet=snippets)
        return df

    def __get_url__(self, keywords=None):
        """
        Get the keywords and bindup in the base urls
        """
        if keywords is not None:
            search_keywords = urllib.parse.quote(keywords)

            if self.hl:
                search_keywords += f"&hl={self.hl}"
            if self.gl:
                search_keywords += f"&gl={self.gl}"

            return self.__get_data_from_url__(
                url=f"{self.base_url}search?q={search_keywords}"
            )
        return

    def __get_data_from_url__(self, url=None):
        """
        parse the data and used date filters
        """
        if url is not None:
            print("url: ",url)
            news_feed = feedparser.parse(url)
            data = pandas.json_normalize(news_feed.entries)
            return self.__filtered_data__(data=data)
        return

    def __filtered_data__(self, data):
        """
        Fiter the data with date ranges and return required response
        """
        if self.start_date and self.end_date and not data.empty is True:
            data = data.filter(items=self.columns)
            published = data["published"].to_list()
            new_date = [
                datetime.strptime(str(i), "%a, %d %b %Y %H:%M:%S GMT").strftime(
                    "%Y-%m-%d"
                )
                for i in published
            ]
            df = data.assign(published=new_date)

            data = df[
                (df["published"] >= self.start_date)
                & (df["published"] <= self.end_date)
            ]
        else:
            data = data
        data = self.__get_link_and_full_text__(data)
        return self.output_data(data=data)

    def __get_link_and_full_text__(self, data):
        """
        this function deals with scrapping the data using links
        """
        if not data.empty is True:
            links = data["link"].to_list()
            detail_link_page = []
            detail_links = list()
            detail_headers = list()    
            for index, link in enumerate(links):
                detail_link = data["link"].iloc[index]
                title = data["title"].iloc[index]
                title_spilt = title.split(" -")
                detail_link_header = title_spilt[0]
                detail_links.append(detail_link)
                detail_headers.append(detail_link_header)

                try:
                    request_response = requests.get(link)
                except:
                    pass
                if request_response.status_code == 200:
                    html_content = request_response.content
                    soup = BeautifulSoup(html_content, "html.parser")
                    detail_link_body = "".join(
                        [
                            "".join(t)
                            for t in str(soup.text).split("\n")
                            if len(t) > 0 and "\t" not in t
                        ]
                    )

                detail_link_page.append(detail_link_body)

            data = pandas.DataFrame(
                    {
                        'title': data['title'],
                        'link': detail_links,
                        'detail_link_body': detail_link_page,
                        'published': data['published'],
                        'header':detail_headers
                    }
                )
        else:
            data=data
        return data

    def output_data(self, data=None):
        """
        Return required Response
        """
        return data


if __name__ == "__main__":
    
    # keywords='hernia repair'
    # keywords='hernie'
    # keywords="hernie"
    # keywords='"hernien"'
    # keywords='hernien'
    # keywords='"hernie" OR "hernien"'
    # keywords='("hernie" OR "hernien")'
    
    # keywords='3D'
    
    def schedule_job(keywords=None):
        print(f"I am using {keywords} keyword")
        try:
            get_location_regions = pycountry.countries.search_fuzzy('India')[0]
            location = (get_location_regions.alpha_2).lower()
            region = get_location_regions.alpha_2
        except:
            print("please Choose correct Region")
            get_location_regions = None
            location = None
            region = None

        obj = GoogleScapper(
            keywords=keywords,
            # start_date="2021-08-12",
            # end_date="2022-08-12",
            location=location,
            region=region
        )
        # obj = obj.start()

        google_news_obj = obj.get_google_news_feed()
        yahoo_obj = obj.get_yahoo_feed()
        bing_obj = obj.get_bing_feed()
        combined_data = pandas.concat([google_news_obj,yahoo_obj, bing_obj], axis=0, ignore_index=True)
        combined_data.to_csv('combined_result.csv', index=False)
        print("scrapped data from google news, yahoo, bing")

    parser = argparse.ArgumentParser()
    parser.add_argument('--scheduleTime', '--scheduleTime', help='--scheduleTime=HH:MM', action='append')

    parser.add_argument('-search', '--search_keywords', help='Please use qutos for search keywork', action='append')

    args = parser.parse_args()
    
    if args.scheduleTime and args.search_keywords:
        scheduleTime = args.scheduleTime[0]
        search_keywords = args.search_keywords[0]
        schedule.every().day.at(scheduleTime).do(lambda:schedule_job(search_keywords))
        with open("search_keywords.txt", "a+") as f:
            f.write(search_keywords + "\n")

        while True:
            schedule.run_pending()
            time.sleep(1)
    elif args.search_keywords:
        search_keyword = args.search_keywords[0]
        schedule_job(search_keyword)
        with open("search_keywords.txt", "a+") as f:
            f.write(search_keyword + "\n")
    else:
        print("Please use python gscrap.py -h command")
