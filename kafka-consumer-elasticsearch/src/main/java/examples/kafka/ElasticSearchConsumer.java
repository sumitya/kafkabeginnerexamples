package examples.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient restClient = getRestClient();

        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer("twitter_tweets");

        //poll the records
        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(200));

            logger.info("Received Recrods: "+records.count());

            for (ConsumerRecord<String, String> record : records) {

                // generate id from the record.
                //String id =  record.topic() + "_" + record.partition() + "_" + record.offset();

                //generate id from the tweet

                String id = extractFromTweet(record.value());

                logger.info("ID returned from the Tweet Value() is: "+id);

                logger.info("Key: " + record.key() + "\t" + "Value: " + record.value());
                logger.info("Partition:" + record.partition() + ", Offset: " + record.offset());

                //Insert data into ElasticSearch

                String tweet = record.value();

                // Making the request idempotent.
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id //
                ).source(tweet, XContentType.JSON);


                IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);

                String responeId = indexResponse.getId();

                logger.info("Response Code from ElasticSearch is: " + responeId);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                logger.info("Committing Offsets...");

                kafkaConsumer.commitAsync();

                logger.info("Offset have been commited...");

            }

            //close the client
            restClient.close();

        }

    }

    private static JsonParser jsonParser = new JsonParser();

    public static String extractFromTweet(String inputJson){

        return jsonParser.parse(inputJson).
                getAsJsonObject().
                get("id_str").
                getAsString();

    }

    public static RestHighLevelClient getRestClient() {

        String hostName = "localhost";
        String userName = "";
        String passsword = "";

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, passsword));

        RestClientBuilder builder = RestClient.builder(

                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(
                        new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        }

                );

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

    public static KafkaConsumer<String, String> getKafkaConsumer(String topic) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "elasticsearch-consumer-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // disable auto commit to false
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }
}
