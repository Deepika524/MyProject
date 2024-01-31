import datetime
import json
import time
import tweepy
from kafka import KafkaProducer

# Twitter API credentials
consumer_key = 'BvBVJh3PzQ0ix59jDfIndyoKm'
consumer_secret = 'td1mVfV8g5cz5rTdCrmwVquPTfPSdb8kxwmIsI5AwB0DhyPLaY'
access_token = '1751509450287161344-VbQhp8nkH4eKePAEZRz7spBshYYFLC'
access_token_secret = 'lIkIG2qkYPyHLwuo2eUuYOJH3QKR3KQNIWG9zZxLnvLYp'

# Kafka broker settings
kafka_brokers = "localhost:9092"
topic_name = 'topic_man1'
important_fields = ['created_at', 'id', 'id_str', 'text', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'lang']

# Create Twitter API object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_brokers,
    api_version=(0, 10, 1)
)

def get_tweets():
    try:
        for tweet in tweepy.Cursor(api.search, q='kafka').items():
            tweet_data = {k: tweet._json[k] for k in tweet._json if k in important_fields}
            tweet_data['text'] = tweet_data.get('text', '').replace("'", "").replace("\"", "").replace("\n", "")
            producer.send(topic_name, str.encode(json.dumps(tweet_data)))
    except tweepy.TweepError as e:
        print(f"Error fetching tweets: {e}")

def stream(interval):
    while True:
        get_tweets()
        print(f'Streaming Tweets at {datetime.datetime.now()}')
        time.sleep(interval)

if __name__ == "__main__":
    try:
        stream(10)
    finally:
        producer.close()
