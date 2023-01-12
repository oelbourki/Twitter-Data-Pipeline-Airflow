import configparser
import tweepy
import pandas as pd
import json
import s3fs
from datetime import datetime

def run_twitter_etl():
	config = configparser.ConfigParser()
	with open('.config', 'r') as config_file:
		config.read_file(config_file)
	access_key = config['access_key'] 
	access_secret = config['access_secret']  
	consumer_key = config['consumer_key'] 
	consumer_secret = config['consumer_secret'] 

	auth = tweepy.OAuthHandler(access_token=access_key, access_token_secret=access_secret)
	auth.set_access_token(consumer_key, consumer_secret)

	api = tweepy.API(auth=auth)
	tweets = api.user_timeline(screen_name='@elonmusk',
			count=200,
			include_rts = False,
			tweet_mode = 'extended')
	
	lst = []
	for tweet in tweets:
		text = tweet.__json['full_text']

		refind_tweet = {
			'user': tweet.user.screen_name,
			'text': text,
			'favorite_count': tweet.favorite_count,
			'retweet_count': tweet.retweet_count,
			'created_at': tweet.created_at
		}
		lst.append(refind_tweet)
	
	df = pd.DataFrame(lst)
	df.to_csv('refined_tweets.csv')