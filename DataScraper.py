from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler
import tweepy
import Credentials
from pykafka import KafkaClient
import json


def get_kafka_client():
    return KafkaClient(hosts='192.168.2.213:9092')


class MyStreamListener(StreamListener):
    def on_data(self, data):
        print(data)
        message = json.loads(data)
        if message['place'] is not None:
            client = get_kafka_client()
            topic = client.topics['twitterdata']
            producer = topic.get_sync_producer()
            producer.produce(data.encode('ascii'))
        return True

    def on_error(self, status_code):
        print(status_code)
        return False


if __name__ == "__main__":
    auth = tweepy.OAuthHandler(Credentials.API_KEY, Credentials.API_KEY_SECRET)
    auth.set_access_token(Credentials.ACCESS_TOKEN, Credentials.ACCESS_TOKEN_SECRET)
    listener = MyStreamListener()
    Stream = Stream(auth, listener)
    search_query = 'Corona Virus'
    Stream.filter(languages=["en"], track=[search_query])
