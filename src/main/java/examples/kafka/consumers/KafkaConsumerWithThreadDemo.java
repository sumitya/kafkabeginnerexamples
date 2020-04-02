package examples.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerWithThreadDemo {

    public static void main(String[] args) {

            new KafkaConsumerWithThreadDemo().run();

    }

    private KafkaConsumerWithThreadDemo(){


    }

    private void run(){

        Logger logger = LoggerFactory.getLogger(KafkaConsumerWithThreadDemo.class.getName());

        //Latch for dealing with Multiple Threads
        CountDownLatch latch = new CountDownLatch(1);

        // Creating a Consumer Thread.
        logger.info("Creating a Consumer Thread here: ");
        Runnable myConsumerThread =  new ConsumerThread(
                latch,
                "localhost:9092",
                "my-first-application");

        //Starting the thread.
        Thread consumerThead = new Thread(myConsumerThread);
        consumerThead.start();

        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()  -> {

                    logger.info("Caught Shutdown Hook");
                    ((ConsumerThread) myConsumerThread).shutDown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Application has exited!!");
                }
            )
        );


        // wait for the thread to finish reading
        try {
            latch.await();

        } catch (InterruptedException e) {

            logger.info("Application Thread interrupted ",e);

            e.printStackTrace();
        } finally {

            logger.info("Application Thread finished reading, closing!!!");

        }
    }

    // inner class for the thread
    static public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());



        public ConsumerThread(CountDownLatch latch,
                              String bootStrapServer,
                              String groupId
        ) {
            this.latch = latch;

            // Create Configs

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            // Create consumer
            consumer = new KafkaConsumer<>(properties);

            consumer.subscribe(Arrays.asList("first_topic"));
        }

        @Override
        public void run() {

            //poll the records
            try {
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + "\t" + "Value: " + record.value());
                        logger.info("Partition:" + record.partition() + ", Offset: " + record.offset());

                    }
                }
            }catch(WakeupException wakeupex){
                    logger.info("Received ShutDown Signal!!!");
            } finally {
                consumer.close();
                //tell the main code , consumer done reading the messages.
                latch.countDown();
            }


        }

        public void shutDown() {

            // this method is called to interrupt consumer.poll(), it will through the wakeUpException,
            consumer.wakeup();

        }
    }

}



