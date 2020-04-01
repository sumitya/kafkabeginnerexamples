package examples.kafka.consumers;

import examples.kafka.producers.KafkaProducerDemo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaProducerDemo.class);

        // Create Configs

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-first-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("first_topic"));

        //poll the records

        while(true){

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(200));

            for(ConsumerRecord<String,String> record: records){
                logger.info("Key: "+ record.key()+"\t"+ "Value: "+ record.value());
                logger.info("Partition:" + record.partition()+ ", Offset: "+ record.offset());

            }
        }
    }
}

