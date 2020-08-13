package bh.spikes.pt.example2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class CustomerEventEnricherApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-event-enricher-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        // Global table out of Kafka. This table will be replicated on each Kafka Streams application
        // the key of our globalKTable is the Customer ID
        GlobalKTable<String, String> customersGlobalTable = builder.globalTable("customer-table");

        // Stream of customer purchases
        KStream<String, String> customerPurchases = builder.stream("customer-purchases");

        // Enrich that stream
        KStream<String, String> customerPurchasesEnrichedJoin =
                customerPurchases.join(customersGlobalTable,
                        (key, value) -> key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
                        (customerPurchase, customerInfo) -> "Purchase=" + customerPurchase + ",CustomerInfo=[" + customerInfo + "]"
                );

        customerPurchasesEnrichedJoin.to("customer-purchases-enriched-inner-join");

        //Enrich that stream using a Left Join
        KStream<String, String> customerPurchasesEnrichedLeftJoin =
                customerPurchases.leftJoin(customersGlobalTable,
                        (key, value) -> key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
                        (customerPurchase, customerInfo) -> {
                            // as this is a left join, customerInfo can be null
                            if (customerInfo != null) {
                                return "Purchase=" + customerPurchase + ",CustomerInfo=[" + customerInfo + "]";
                            } else {
                                return "Purchase=" + customerPurchase + ",CustomerInfo=null";
                            }
                        }
                );

        customerPurchasesEnrichedLeftJoin.to("customer-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}