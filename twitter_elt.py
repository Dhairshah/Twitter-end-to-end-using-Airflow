import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import csv

def run_twitter_etl():
    
    #Access key formatting
    access_key = "WnYMBcOp6v9srUCI7if4GHMM1"
    access_secret = "ZhbiaWo44vQNZxqKTSH1bL9KQm9dFhibJ8Rw7YARgScqwrd6HW"
    consumer_key = "2320639962-Phc02dS9tYXjozPGORlOa0eG6mf9L4k6sDHA2RQ"
    consumer_secret = "bCdlK7AdYfzWRf4mLQn19Cza3Pofwsq1VVZ2DhxBKzftt"


    #Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Creating an API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                            #200 limit
                            count = 200,
                            include_rts = False,
                            tweet_mode = 'extended'
                                )

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('refined_tweets.csv')