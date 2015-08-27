# How to run#


You should use the python script in the /scripts folder to collect tweets. Once you have a collection of tweets, if you want to train a NaiveBayes model run something like:

	spark-submit \
		--class "com.velankani.apps.connectxml.NaiveBayesTrain" \
		--master "local[*]" \
		target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
		"${YOUR_TWEET_INPUT:-alltweets.txt}" \
		${OUTPUT_MODEL_DIR:-naivebayesmodel}

The master is either local or the URL of your spark master. The Tweet input is the collection of labeled Tweets. The output directory is where you want the model to be written to. These directories can be either local or HDFS.

If you have a model and want to categorize livesteamed data and insert them into MongoDB and Neo4j, run something like

	spark-submit \
     	--class "com.velankani.apps.connectxml.NaiveBayesPredict" \
     	--master "local[*]" \
     	target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     	${YOUR_MODEL_DIR:-naivebayesmodel} \
     	${FILE_USED_FOR_TFIDF:-alltweets.txt} \
     	--consumerKey YOUR_KEY \
     	--consumerSecret YOUR_SECRET \
     	--accessToken YOUR_TOKEN  \
     	--accessTokenSecret YOUR_TOKEN_SECRET

If you want to run the tweet bot analyzer do something like

	spark-submit \
 		--class "com.velankani.apps.connectxml.TwitterTimelineCheck"  \
		--master "local[*]" \
 		target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
 		YOUR_KEY \
 		YOUR_SECRET \
 		YOUR_TOKEN \
 		YOUR_TOKEN_SECRET
