package examples.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithKeysDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Create an instance of a logger
        final Logger logger  = LoggerFactory.getLogger(KafkaProducerDemo.class);

        // Create Producer Properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        int i;
        for(i=0;i<10;i++){

        // Create a Key, Value
        String key = Integer.toString(i);
        String value = "Hello World on Wedndesday " + Integer.toString(i);
        // Create a producer record

        ProducerRecord<String,String> record = new ProducerRecord<>("first_topic",key,value);

        // send the data  - this is asynchronous

            logger.info("Key: "+ key);
        producer.send(record, new Callback() {
            // executes every time a record is successfully sent or an exception is thrown.
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
                if(ex == null){

                    // record sent was success
                    logger.info("Received Records Metadata\n"+
                            "Topic: "+ recordMetadata.topic() +"\n" +
                            "Partition: "+ recordMetadata.partition() +"\n" +
                            "Offset: "+ recordMetadata.offset() +"\n" +
                            "TimeStamp: "+ recordMetadata.timestamp() +"\n"


                    );

                }
                else{
                    logger.error("Error while producing the record: "+ ex);
                }

            }
        }).get();  // This blocks the .send(), to make it synchronous.


        }


    }

}
