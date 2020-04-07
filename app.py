import tweepy
from google.cloud import pubsub_v1


with open(gs://mycredentials-rw/key.txt) as f:
  MYKEY = f.read()
with open(gs://mycredentials-rw/keysecret.txt) as f:
  MYKEYSECRET = f.read()
with open(gs://mycredentials-rw/token.txt) as f:
  MYTOKEN = f.read()
with open(gs://mycredentials-rw/tokensecret.txt) as f:
  MYTOKENSECRET = f.read()
  
MYKEYSECRET = gs://mycredentials-rw/xxx
MYTOKEN = gs://mycredentials-rw/token.txt
MYTOKENSECRET = gs://mycredentials-rw/

#!gsutil cp $MYKEY 

# Authenticate
auth = tweepy.OAuthHandler(YOURKEY, YOURSECRET)
auth.set_access_token(YOURKEY, YOURSECRET)

# Configure to wait on rate limit if necessary
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)

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
