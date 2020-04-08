#   Created: 04/07/20
#   Author: Remy Welch 
#
# Steps:
#   1) create VM on GCP (n1standard2), use initialization script in install python/git: 	
#   !  /bin/bash
#   sudo apt-get update
#   sudo apt-get install python3-pip
#   sudo apt-get install git
#   pip3 install tweepy
#   pip3 install google-cloud-storage
#   pip3 install google-cloud-pubsub
#   mkdir /home/remyw/keys
#   gsutil cp -r gs://mycredentials-rw/ /home/remyw/keys
#   2) put your twitter developer tokens/secrets in a GCS bucket (or copy them to the VM by some other means)
#   3) execute: git clone https://github.com/remylouisew/twitter_streaming.git
#   4) run this file: $python3 /home/remyw/twitter_streaming/app.py
#   4) NEXT: run this file from (find path)...see if it works


import tweepy
from google.cloud import pubsub_v1 as pub
from google.cloud import storage as gcs

#!gsutil cp -r gs://mycredentials-rw/keysecret.txt /home/remyw/keysecret.txt
#!gsutil cp -r gs://mycredentials-rw/token.txt /home/remyw/token.txt
#!gsutil cp -r gs://mycredentials-rw/tokensecret.txt /home/remyw/tokensecret.txt
  
with open("/home/remyw/keys/key.txt") as a:
  MYKEY = a.read() 
with gcs.open("/home/remyw/keys/keysecret.txt") as b:
  MYKEYSECRET = b.read()
with gcs.open("/home/remyw/keys/token.txt") as c:
  MYTOKEN = c.read()
with gcs.open("/home/remyw/keys/tokensecret.txt") as d:
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

# Listener class
class TweetListener(StreamListener):

    def __init__(self):
        super(StdOutListener, self).__init__()

    def on_status(self, data):
        # When receiveing a tweet: send it to pubsub
        write_to_pubsub(reformat_tweet(data._json))
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False
          
# Make an instance of the class
l = TweetListener()

# Start streaming
stream = tweepy.Stream(auth, l, tweet_mode='extended')
stream.filter(track=lst_hashtags)


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
                "user_id": data["user_id"],
                "id": data["id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
            
    except Exception as e:
        print(e)
        raise
