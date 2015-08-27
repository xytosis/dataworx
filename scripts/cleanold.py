import pymongo
from pymongo import MongoClient
import datetime
import sys

def main(argv):
	if len(argv) != 5:
		print "usage: python cleanold.py <host> <port> <dbname> <collectionname>"
		sys.exit()
	host = argv[1]
	port = int(argv[2])
	dbname = argv[3]
	collectionname = argv[4]
	client = MongoClient(host, port)
	db = client[dbname]
	tweets = db[collectionname]
	dateFormat = "%a %b %d %H:%M:%S %Z %Y"
	d = datetime.datetime.now() - datetime.timedelta(days=30)
	# allTweets = tweets.find({"createdAt": {"$exists":"true"}}, {"createdAt":1})
	# idToTime = filter(lambda x: x[1] <= d,map(lambda x: (x["_id"], datetime.datetime.strptime(x["createdAt"], dateFormat)), allTweets))
	# # deletes all the tweets that are more than 30 days old
	# map(lambda x: tweets.remove({"_id": x[0]}), idToTime)
	tweets.remove({"createdAt": {"$lt":d}})

if __name__ == '__main__':
	main(sys.argv)