from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy
import datetime
import json
import pandas as pd
import csv
import re
from textblob import TextBlob
import string
import preprocessor as p
import os
import time

consumer_key = 'your_consumer_key'
consumer_secret = 'your_consumer_secret'
access_key = 'your_access_key'
access_secret = 'your_access_secret'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

def ScrapTweets (search_words, date_until, num_tweets, num_runs, tweet_max_id = 0):
    program_start = time.time()
    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags', 'id_int'])
    
    for i in range(0, num_runs): 
        start_run = time.time()
        
        if i > 0:
            last_tweet_id = (db_tweets['id_int'].iloc[-1] - 1)
            tweets = tweepy.Cursor(api.search, q=search_words, lang="en", max_id=last_tweet_id, tweet_mode='extended').items(num_tweets)
        elif tweet_max_id != 0:
            tweets = tweepy.Cursor(api.search, q=search_words, lang="en", max_id=(tweet_max_id - 1), tweet_mode='extended').items(num_tweets)
        else:
            tweets = tweepy.Cursor(api.search, q=search_words, lang="en", until=date_until, tweet_mode='extended').items(num_tweets)

        
        tweet_list = [tweet for tweet in tweets]
        
        noTweets = 0
                        
        for tweet in tweet_list:

            username = tweet.user.screen_name
            acctdesc = tweet.user.description
            location = tweet.user.location
            following = tweet.user.friends_count
            followers = tweet.user.followers_count
            totaltweets = tweet.user.statuses_count
            usercreatedts = tweet.user.created_at
            tweetcreatedts = tweet.created_at
            retweetcount = tweet.retweet_count
            hashtags = tweet.entities['hashtags']
            id_int = tweet.id

            try:
                text = tweet.retweeted_status.full_text
            except AttributeError:  
                text = tweet.full_text

            ith_tweet = [username, acctdesc, location, following, followers, totaltweets,
                         usercreatedts, tweetcreatedts, retweetcount, text, hashtags, id_int]

            db_tweets.loc[len(db_tweets)] = ith_tweet
 
            noTweets += 1
            

        end_run = time.time()
        duration_run = end_run-start_run

        print('Number of scrapped tweets in run {} are {}'.format(i + 1, noTweets))
        print('Time taken to scrape run {} was {} minutes'.format(i+1, round((duration_run)/60,2)))                        
        if duration_run < 900 and (i+1) != num_runs:
            time.sleep(900-duration_run) 
            
    from datetime import datetime

    to_csv_timestamp = datetime.today().strftime('%Y-%m-%d_%H%M%S')

    path = os.getcwd()
    filename = path + to_csv_timestamp + '_scraping_tweets.csv'

    db_tweets.to_csv(filename, index = False)
    
    program_end = time.time()
    print('Scraping complete!')
    print('Total time taken was {} minutes.'.format(round(program_end - program_start)/60, 2))
