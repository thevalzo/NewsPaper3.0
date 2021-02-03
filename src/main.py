import pymongo
from pymongo import MongoClient
import json
import feedparser
import urllib.request
import html2text
import re
import json
import tweepy
from cleantext import clean

from BaseTextCleaner import BaseTextCleaner
from Crawler import Crawler

def main ():
    #Setup MongoDB
    inp = open('..\..\connection_string.txt', 'r', encoding='utf-8')

    client = eval('pymongo.MongoClient(\'{}\')'.format(inp.readline().strip()))

    inp.close()

    # Setup Twitter API
    with open('..\..\connection.json', 'r') as file:
        data = json.load(file)
    file.close()

    auth = tweepy.OAuthHandler(data["OAuthHandler"][0], data["OAuthHandler"][1])
    auth.set_access_token(data["AccessToken"][0], data["AccessToken"][1])

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    #Start crawling
    crawler = Crawler(api)

    db = client.test

    feed_sources = db["feed_sources"]
    crawler.update_collection(db, 'feed_sources')
    tweet_sources = db["tweet_sources"]
    crawler.update_collection(db, "tweet_sources")

    feed_posts = db["feed_post"]
    #feed_posts.create_index([("link", pymongo.DESCENDING)], unique=True)
    tweets = db["tweets"]
    #tweet.create_index([("link", pymongo.DESCENDING)], unique=True)
    articles = db["articles"]
    #articles.create_index([("link", pymongo.DESCENDING)], unique=True)

    crawler.crawl_feed_posts(feed_sources, feed_posts)
    #db.feed_post.create_index([("link", pymongo.DESCENDING)], unique=True)

    crawler.crawl_tweets(tweet_sources, tweets)
    #db.tweets.create_index([("id_str", pymongo.DESCENDING)], unique=True)
    #db.articles.create_index([("link", pymongo.DESCENDING)], unique=True)
    crawler.crawl_post_link(feed_posts, articles)


    crawler.crawl_tweet_link(tweets, articles)


    #basetextcleaner = BaseTextCleaner(db)
    #basetextcleaner.clean_1()
    #basetextcleaner.clean_2_update_clean_info()
    #basetextcleaner.clean_2()

if __name__ == '__main__':
    main()

