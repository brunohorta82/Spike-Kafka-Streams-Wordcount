package bh.spikes.pt.example2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomerDataProducer {


    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // FYI - We do .get() to ensure the writes to the topics are sequential, for the sake of the teaching exercise
        // DO NOT DO THIS IN PRODUCTION OR IN ANY PRODUCER. BLOCKING A FUTURE IS BAD!
        
        // we are going to test different scenarios to illustrate the join

        // 1 - we create a new customer, then we send some data to Kafka
        System.out.println("\nExample 1 - new customer\n");
        producer.send(customerRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

        Thread.sleep(10000);

        // 2 - we receive customer purchase, but it doesn't exist in Kafka
        System.out.println("\nExample 2 - non existing customer\n");
        producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();

        Thread.sleep(10000);

        // 3 - we update customer "john", and send a new transaction
        System.out.println("\nExample 3 - update to customer\n");
        producer.send(customerRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();

        Thread.sleep(10000);

        // 4 - we send a customer purchase for stephane, but it exists in Kafka later
        System.out.println("\nExample 4 - non existing customer then customer\n");
        producer.send(purchaseRecord("stephane", "Computer (4)")).get();
        producer.send(customerRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).get();
        producer.send(purchaseRecord("stephane", "Books (4)")).get();
        producer.send(customerRecord("stephane", null)).get(); // delete for cleanup

        Thread.sleep(10000);

        // 5 - we create a customer, but it gets deleted before any purchase comes through
        System.out.println("\nExample 5 - customer then delete then data\n");
        producer.send(customerRecord("alice", "First=Alice")).get();
        producer.send(customerRecord("alice", null)).get(); // that's the delete record
        producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();

        Thread.sleep(10000);

        System.out.println("End of demo");
        producer.close();
    }

    private static ProducerRecord<String, String> customerRecord(String key, String value) {
        return new ProducerRecord<>("customer-table", key, value);
    }


    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("customer-purchases", key, value);
    }
}