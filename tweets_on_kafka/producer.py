from kafka import KafkaProducer
from os import environ

import tweepy
import json
import logging


class KafkaStreamListener(tweepy.StreamListener):
    def __init__(self, producer, topic):
        super(KafkaStreamListener, self).__init__()
        self._producer = producer
        self._topic = topic

    def on_data(self, raw_data):
        self._producer \
            .send(self._topic, json.loads(raw_data)) \
            .add_callback(on_send_success) \
            .add_errback(on_send_error)

        logging.info(raw_data)

        return True

    def on_error(self, status):
        logging.error(status)


def on_send_success(record_metadata):
    logging.info((record_metadata.topic, record_metadata.partition, record_metadata.offset))


def on_send_error(e):
    logging.error('Issue sending message', exc_info=e)


def start_sending_message(bootstrap_servers, topic, track):
    auth = tweepy.OAuthHandler(environ['CONSUMER_API_KEY'], environ['CONSUMER_API_SECRET'])
    auth.set_access_token(key=environ['ACCESS_TOKEN'], secret=environ['ACCESS_TOKEN_SECRET'])

    kafka_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda m: json.dumps(m).encode('utf8'),
        retries=5
    )

    kafka_stream_listener = KafkaStreamListener(
        producer=kafka_producer,
        topic=topic
    )

    tweepy_stream = tweepy.Stream(auth=auth, listener=kafka_stream_listener)

    logging.info('Starting twitter stream...')
    tweepy_stream.filter(track=track)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    producer_logger = logging.getLogger('producer')
    start_sending_message(
        bootstrap_servers=['192.168.0.12:9092'],
        topic='test1',
        track=['weather']
    )
