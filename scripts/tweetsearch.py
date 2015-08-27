# Class to store tweet information
import oauth2
import urllib2
import json
import sys

class Tweet:
    def __init__(self, date, idd, text, username, location):
        self.date = date
        self.id = idd
        self.text = text.replace("\n", " ")
        self.username = username.replace("\n", " ")
        self.location = location.replace("\n", "")

    def __repr__(self):
        return (self.username.encode("utf8") + "|#|" + str(self.id) + "|#|"
            + self.date.encode('utf8') + "|#|" + self.location.encode("utf8")
            + "|#|" + self.text.encode('utf8') + "|#|" + "0" + "\n")


TWITTER_CONSUMER_KEY = TODO
TWITTER_CONSUMER_SECRET = TODO
TWITTER_TOKEN = TODO
TWITTER_TOKEN_SECRET = TODO



# Searches for target in twitter and returns a list of tweet objects
def search(target):
    url_params = {
    "q": target,
    "result_type": "recent"
    }
    url = "https://{0}{1}?".format("api.twitter.com", "/1.1/search/tweets.json")
    consumer = oauth2.Consumer(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    oauth_request = oauth2.Request(method="GET", url=url, parameters=url_params)
    oauth_request.update({
        "oauth_nonce": oauth2.generate_nonce(),
        "oauth_timestamp": oauth2.generate_timestamp(),
        "oauth_token": TWITTER_TOKEN,
        "oauth_consumer_key": TWITTER_CONSUMER_KEY
        })
    token = oauth2.Token(TWITTER_TOKEN, TWITTER_TOKEN_SECRET)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    signed_url = oauth_request.to_url()
    try:
        conn = urllib2.urlopen(signed_url, None)
    except Exception, e:
        return
    try:
        response = json.loads(conn.read())
    finally:
        conn.close()
    tweets = []
    for tweet in response["statuses"]:
        tweets.append(Tweet(tweet["created_at"], tweet["id"], tweet["text"], tweet["user"]["name"]
            , tweet["user"]["location"]))
    return tweets


# Writes to file a collection of tweet objects. Each field in the tweet object is
# separated by |#| to make splitting easier
def main():
    if len(sys.argv) != 3:
        print "Usage: python tweetsearch.py <search string> <file to save at>"
        return

    results = search(sys.argv[1])
    myfile = open(sys.argv[2], "a+")
    for t in results:
        myfile.write(str(t))
    myfile.close()

if __name__ == '__main__':
    main()
