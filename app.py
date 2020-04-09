#   Created: 04/07/20
#   Author: Remy Welch 

'''
#   Tweet documentation: https://developer.twitter.com/en/docs/tweets/curate-a-collection/api-reference/get-collections-entries
#
# Steps:
 1) create VM on GCP (n1standard2), use initialization script in install python/git: 	
   !  /bin/bash
   sudo apt-get update
   sudo apt-get install python3-pip
   sudo apt-get install git
   pip3 install tweepy
   pip3 install google-cloud-storage
   pip3 install google-cloud-pubsub
   mkdir /home/remyw/keys
   gsutil cp -r gs://mycredentials-rw/ /home/remyw/keys
1.5) Use service account twitter-access...VM needs pubsub editor access, etc (just went ahead and gave it project editor access)
2) put your twitter developer tokens/secrets in a GCS bucket (or copy them to the VM by some other means)
3) execute: git clone https://github.com/remylouisew/twitter_streaming.git
4) run this file: $ screen -S streaming_script python3 /home/remyw/twitter_streaming/app.py
    https://linuxize.com/post/how-to-use-linux-screen/
4) NEXT: 

'''

import datetime
import json
import time
import tweepy
from google.cloud import pubsub_v1
from google.cloud import storage as gcs
from tweepy.streaming import StreamListener

#!gsutil cp -r gs://mycredentials-rw/keysecret.txt /home/remyw/keysecret.txt
#!gsutil cp -r gs://mycredentials-rw/token.txt /home/remyw/token.txt
#!gsutil cp -r gs://mycredentials-rw/tokensecret.txt /home/remyw/tokensecret.txt
  
with open("/home/remyw/keys/mycredentials-rw/key.txt") as a:
  MYKEY = a.read() 
with open("/home/remyw/keys/mycredentials-rw/keysecret.txt") as b:
  MYKEYSECRET = b.read()
with open("/home/remyw/keys/mycredentials-rw/token.txt") as c:
  MYTOKEN = c.read()
with open("/home/remyw/keys/mycredentials-rw/tokensecret.txt") as d:
  MYTOKENSECRET = d.read()
  

#!gsutil cp $MYKEY 

# Authenticate
auth = tweepy.OAuthHandler(MYKEY, MYKEYSECRET)
auth.set_access_token(MYTOKEN, MYTOKENSECRET)

# Configure to wait on rate limit if necessary
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)

try:
    api.verify_credentials()
    print("Authentication OK")
except:
    print("Error during authentication")

# Hashtag list
lst_hashtags = ["#got", "#gameofthrones"]

'''
       PubSub Notes:
          - When using JSON over REST, message data must be base64-encoded
          - Messages must be smaller than 10MB (after decoding)
          - The message payload must not be empty
          - Attributes can also be added to the publisher payload

'''

# Send the data to PubSub
MY_PROJECT = "twitter-stream-rw"
MY_PUBSUB_TOPIC = "twitter1"

# Configure the connection
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(MY_PROJECT, MY_PUBSUB_TOPIC)

# Function to write data to
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
          
            # publish to the topic, don't forget to encode everything at utf8!
            publisher.publish(topic_path, data=json.dumps({
                "text": data["text"],
                "user": data["user"],
                "id": data["id"],
                "place": data["place"] if data["place"] else None,
                "retweeted_status": data["retweeted_status"] if "retweeted_status" in data else None,
                "favorite_count": data["favorite_count"] if "favorite_count" in data else 0,
                "retweet_count": data["retweet_count"] if "retweet_count" in data else 0,
                "coordinates": data["coordinates"],
                "created_at": time.mktime(time.strptime(data["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
            
    except Exception as e:
        print(e)
        raise
        
def pubsub_callback( message_future ):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('[ ERROR ] Publishing message on {} threw an Exception {}.'.format(topic_name, message_future.exception()))
    else:
        print('[ INFO ] Result: {}'.format(message_future.result()))


#can choose to stream directly to BQ instead (have not tested)
def stream_to_bq( bq_client, bq_table, data ):
    '''
        bq_client    = bigquery.Client()
        bq_table_ref = client.dataset(bq_dataset_id).table(bq_table_id)
        bq_table     = client.get_table(table_ref)
    '''
    errors    = bq_client.insert_rows(bq_table, [tuple([ v for k,v in data.items() ])] )
    if errors == []:
        print('[ INFO ] Complete. Successfully inserted records into BigQuery')
    else:
        print('[ WARNING ] Failed to write records to BigQuery')
    
    return None
  
#Custom Listener class
class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just pushes tweets to pubsub
    """

    def __init__(self):
        super(StdOutListener, self).__init__()
        self._counter = 0

    def on_status(self, data):
        write_to_pubsub(data._json)
        self._counter += 1
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False
          
# Make an instance of the class
l = StdOutListener()

# Start streaming
stream = tweepy.Stream(auth, l, tweet_mode='extended')
stream.filter(track=lst_hashtags)

