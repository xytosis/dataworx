spark-submit \
     --class "com.velankani.apps.connectxml.NaiveBayesPredict" \
     --master "local[*]" \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     ${YOUR_MODEL_DIR:-naivebayesmodel2} \
     ${FILE_USED_FOR_TFIDF:-alltweets.txt} \
     ${KAFKA_BROKERS:-localhost:9092,localhost:9093,localhost:9094} \
     ${KAFKA_TOPICS:-zerg.hydra} \
     --consumerKey YOUR_CONSUMER_KEY \
     --consumerSecret YOUR_CONSUMER_SECRET \
     --accessToken YOUR_ACCESS_TOKEN  \
     --accessTokenSecret YOUR_ACCESS_SECRET
