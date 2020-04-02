package examples.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerAssignSeekDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(KafkaConsumerAssignSeekDemo.class.getName());

        // Create Configs

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign and seek - used for replay the data or fetch a specific message.

        TopicPartition topicPartition = new TopicPartition("first_topic", 2);
        long offsetToReadFrom = 15L;

        //assign
        consumer.assign(Arrays.asList(topicPartition));

        //assign
        consumer.seek(topicPartition, offsetToReadFrom);

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + "\t" + "Value: " + record.value());
                logger.info("Partition:" + record.partition() + ", Offset: " + record.offset());

            }
        }
    }
}








