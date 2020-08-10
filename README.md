# Spike-Kafka-Streams-Wordcount
# Download Kafka binaries
https://kafka.apache.org/downloads

# Start zookeper
zookeeper-server-start.sh ../config/zookeeper.properties

# Start kafka
kafka-server-start.sh ../config/server.properties

# Start Kafka producer
kafka-console-producer.sh --broker-list localhost:9092 --topic words

# Start Kafka consumers

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic words

kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic word-count-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
    
# Run Java Class on Kafka
 sh kafka-run-class.sh WordCountApp.java

# Package Application on Fat Jar
mvn package
