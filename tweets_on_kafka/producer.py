from kafka import KafkaProducer
from kafka.errors import KafkaError
from os import environ

import tweepy


def start_producer():
    auth = tweepy.OAuthHandler(environ['CONSUMER_API_KEY'], environ['CONSUMER_API_SECRET'])
    auth.set_access_token(environ['ACCESS_TOKEN'], environ['ACCESS_TOKEN_SECRET'])

    api = tweepy.API(auth)

    public_tweets = api.home_timeline()
    for tweet in public_tweets:
        print(tweet.text)

    producer = KafkaProducer(bootstrap_servers=['localhost:32768'])


if __name__ == '__main__':
    start_producer()
