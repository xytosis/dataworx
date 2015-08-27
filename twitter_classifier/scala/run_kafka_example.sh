spark-submit \
 --class "com.velankani.apps.connectxml.KafkaTest" \
 --master "local[*]" \
 target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar  \
 KAFKA_BROKERS_TODO \
zerg.hydra
