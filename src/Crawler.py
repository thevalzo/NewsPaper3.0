import feedparser
import html2text
import urllib.request
import pymongo
import datetime
import nltk
nltk.download('punkt')
from newspaper import Article
import json
import requests

class Crawler:

    feed_urls = {}
    feed_post = {}
    articles = {}

    def __init__(self, api):
        self.api = api
  #      self.feed_urls = db["feed_urls"]
  #      self.tweet_urls = db["tweet_urls"]
  #      self.feed_post = db["feed_post"]
  #      self.feed_post.create_index([("link", pymongo.DESCENDING)],unique=True)
  #      self.articles = db["articles"]
  #      self.articles.create_index([("link", pymongo.DESCENDING)],unique=True)

#GENERAL FUNCTIONS
    def update_collection(self, db, coll_name):
        collection = db[coll_name]
        collection.drop()

        with open('..\data\\'+coll_name+'.json', 'r') as file:
            data = json.load(file)
        file.close()

        collection = db[coll_name]
        if data != []:
            collection.insert_many(data)

#FUNCTION FOR FEED SOURCES

    def crawl_feed_posts(self, feed_urls, feed_posts):

        for feed in feed_urls.find():
            feed_stream = feedparser.parse(feed['feed_url'])


            count_inserted = 0
            count_duplicate = 0
            for entry in feed_stream.entries:
                if "link" in entry.keys():
                    entry['source'] = feed['feed_name']
                    entry['crawled'] = 'N'
                    entry['link_is_valid'] = 'Y'
                    entry['insert_date'] = datetime.datetime.now()
                    entry['link'] = entry['link'].split('?')[0]
                    try:
                        feed_posts.insert_one(entry)
                        count_inserted += 1
                    except Exception as e:
                        if type(e).__name__ == 'DuplicateKeyError':
                            count_duplicate += 1
                        else:
                            raise e
            print("Feed "+feed['feed_name']+':'+str(count_inserted)+" new post, "+str(count_duplicate) + " duplicate post")


    def crawl_post_link(self, feed_posts, articles):
        h = html2text.HTML2Text()
        h.ignore_links = True

        count_art = 0
        count_updated = 0
        for post in feed_posts.find({
            "crawled": "N",
            "link_is_valid": "Y",
            "link": {"$exists": True}}):
            # add language in feed

            article_link = post["link"].split('?')[0].strip()
            r = requests.head(article_link, allow_redirects=True)
            article_link = r.url.split('?')[0].strip()

            article = Article(article_link, language='it')

            try:
                # fp = urllib.request.urlopen(post["link"])
                article.download()
                article.parse()
            except Exception as ex:
                if type(ex).__name__ == 'HTTPError' or type(ex).__name__ == 'ConnectionResetError' or type(ex).__name__ =='ArticleException':
                    feed_posts.update({"_id": post.get('_id')},{ "$set":  {"link_is_valid": "N"}}, upsert=False)
                    print("Url " + article_link + " non raggiungibile")
                    article = None
                else:
                    raise ex
            if article is not None:

                summary = h.handle(post['summary'])
                categories_temp  = [ x for x in article_link.split('/') if '' != x ]
                categories = categories_temp[2:-1]

                if categories.__len__() != 0:
                    main_category = categories[0]
                    other_categories = categories[1:]
                else:
                    main_category = ""
                    other_categories = []

                article.nlp()

                tags=[]
                feed_authors=[]
                if "tags" in post.keys():
                    tags=post["tags"]
                if "authors" in post.keys():
                    feed_authors=post["authors"]



                article_dict = {"source": post['source'],
                            "title": post['title'].strip(),
                            "summary": summary.strip(),
                            "summary2" : article.summary,
                            "clean1_text": article.text.strip(),
                            "status": "clean1_text",
                            "link": article_link,
                            "feed_authors" : feed_authors,
                            "article_authors" : article.authors,
                            "tags": tags,
                            "keywords" : article.keywords,
                            "insert_date" : datetime.datetime.now(),
                            "category" : main_category,
                            "other_categories" : other_categories,
                            "top_image" : article.top_image,
                            "movies" : article.movies,
                            "publish_date" : article.publish_date,
                            "is_from_RSS": "Y"
                           }

                try:
                    feed_posts.update({"_id": post.get('_id')}, {"$set": {"crawled": "Y", "categories": categories[2:-1] }}, upsert=False)
                    articles.insert_one(article_dict)
                    count_art += 1
                except Exception as e:
                    if type(e).__name__ == 'DuplicateKeyError':
                        print("Url "+article_link+" has been already crawled")
                        articles.update({"link": article_link.strip()},
                                              {"$set": {
                                                  "source": post['source'],
                                                  "title": post['title'].strip(),
                                                  "summary": summary.strip(),
                                                  "summary2": article.summary,
                                                  "text": article.text.strip(),
                                                  "status": "clean1_text",
                                                  "link": article_link,
                                                  "feed_authors": feed_authors,
                                                  "article_authors": article.authors,
                                                  "tags": tags,
                                                  "keywords": article.keywords,
                                                  "insert_date": datetime.datetime.now(),
                                                  "category": main_category,
                                                  "other_categories": other_categories,
                                                  "top_image": article.top_image,
                                                  "movies": article.movies,
                                                  "publish_date": article.publish_date,
                                                  "is_from_RSS": "Y"
                                              }}, upsert=False)
                        count_updated += 1

                    else:
                        raise e


        print("crawled " + str(count_art) + " articles  from feed posts and updated " + str(count_updated) +"articles")

# FUNCTION FOR TWEET SOURCES
    def crawl_tweets(self, tweet_sources, tweets):
        for source in tweet_sources.find():
            count_inserted = 0
            count_duplicate = 0
            tweets_feed = self.api.user_timeline(screen_name=source['username'], count=200, tweet_mode="extended")

            # prepare a cursor object using cursor() method

            for tweet in tweets_feed:
                obj = {
                "id_str" : tweet.id_str,
                 "full_text": tweet.full_text,
                 "hashtags": tweet.entities['hashtags'],
                 "user_mentions": tweet.entities['user_mentions'],
                 "link": tweet.entities['urls'],
                 "retweet_count": tweet.retweet_count,
                 "favorite_count": tweet.favorite_count,
                 "in_reply_to_status_id": tweet.in_reply_to_status_id,
                 "in_reply_to_status_id_str": tweet.in_reply_to_status_id_str,
                 "in_reply_to_screen_name": tweet.in_reply_to_screen_name,
                 "in_reply_to_user_id": tweet.in_reply_to_user_id,
                 "in_reply_to_user_id_str": tweet.in_reply_to_user_id_str,
                 "insert_date" : datetime.datetime.now(),
                 "user_name" : source['username'],
                 "crawled":"N",
                "link_is_valid" : "Y"

                }

                try:
                    tweets.insert_one(obj)
                    count_inserted += 1
                except Exception as e:
                    if type(e).__name__ == 'DuplicateKeyError':
                        count_duplicate += 1
                    else:
                        raise e
            print("Twitter account " + source['username'] + ':' + str(count_inserted) + " new post, " + str(
                count_duplicate) + " duplicate tweet")


    def crawl_tweet_link(self, tweet_collection, articles):

        count_art = 0
        updated_art = 0
        for tweet in tweet_collection.find({
            "crawled": "N",
            "link_is_valid": "Y",
            "link": {"$exists": True}}):
            # add language in feed
            for link in tweet['link']:

                article_link = link['expanded_url'].split('?')[0].strip()
                try:
                    r = requests.head(article_link, allow_redirects=True)

                    article_link = r.url.split('?')[0].strip()

                    article = Article(article_link, language='it')


                    article.download()
                    article.parse()
                except Exception as ex:
                    if type(ex).__name__ in ('HTTPError', 'ConnectionResetError', 'ArticleException','ConnectionError') :
                        tweet_collection.update({"_id": tweet.get('_id')},{ "$set":  {"link_is_valid": "N"}}, upsert=False)
                        print("Url " + article_link + " non raggiungibile")
                        article = None
                    else:
                        raise ex
                if article is not None:
                    article.parse()
                    categories_temp = [x for x in article_link.split('/') if '' != x]
                    categories = categories_temp[2:-1]

                    if categories.__len__() != 0:
                        main_category = categories[0]
                        other_categories = categories[1:]
                    else:
                        main_category = ""
                        other_categories = []

                    article.nlp()



                    article_dict = {"source": tweet['user_name'],
                                    "user_name": tweet['user_name'],
                                    "title": article.title,
                                    "summary2": article.summary,
                                    "clean1_text": article.text.strip(),
                                    "status": "clean1_text",
                                    "link": article_link,
                                    "article_authors": article.authors,
                                    "keywords": article.keywords,
                                    "insert_date": datetime.datetime.now(),
                                    "category": main_category,
                                    "other_categories": other_categories,
                                    "top_image": article.top_image,
                                    "movies": article.movies,
                                    "publish_date": article.publish_date,
                                    "retweet_count": tweet['retweet_count'],
                                    "favorite_count": tweet['favorite_count'],
                                    "hashtags": tweet['hashtags'],
                                    "user_mentions" : tweet['user_mentions'],
                                    "is_from_twitter" : "Y"
                                    }

                    try:
                        tweet_collection.update({"_id": tweet.get('_id')}, {"$set": {"crawled": "Y", "categories": categories[2:-1], "link_is_valid": "Y"}},
                                              upsert=False)
                        articles.insert_one(article_dict)
                        count_art += 1
                    except Exception as e:
                        if type(e).__name__ == 'DuplicateKeyError':
                            print("Url " + article_link + " has been already crawled")
                            articles.update({"link": article_link.strip()},
                                                 {"$set": {
                                                   "user_name": tweet['user_name'],
                                                    "title": article.title,
                                                    "summary2": article.summary,
                                                    "clean1_text": article.text.strip(),
                                                    "status": "clean1_text",
                                                    "link": article_link,
                                                    "article_authors": article.authors,
                                                    "keywords": article.keywords,
                                                    "insert_date": datetime.datetime.now(),
                                                    "category": main_category,
                                                    "other_categories": other_categories,
                                                    "top_image": article.top_image,
                                                    "movies": article.movies,
                                                    "publish_date": article.publish_date,
                                                    "retweet_count": tweet['retweet_count'],
                                                    "favorite_count": tweet['favorite_count'],
                                                    "hashtags": tweet['hashtags'],
                                                    "user_mentions" : tweet['user_mentions'],
                                                     "is_from_twitter": "Y"
                                                 }}, upsert=False)

                        else:
                            raise e

        print("crawled " + str(count_art) + " new  articles  from tweet and updated "+str(updated_art)+" articles")