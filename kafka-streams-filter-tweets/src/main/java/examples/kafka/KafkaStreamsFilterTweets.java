package examples.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsFilterTweets {

    public static void main(String[] args) {
        // create properties

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "first-stream-application");

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic

        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractFromTweet(jsonTweet) > 10000
                // filter popular tweets
        );

        filteredStream.to("filtered_tweets");

        // build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        // start our stream application

        kafkaStreams.start();

    }

    private static JsonParser jsonParser = new JsonParser();

    public static Integer extractFromTweet(String inputJson) {

        try {
            return jsonParser.parse(inputJson).
                    getAsJsonObject().
                    get("user").
                    getAsJsonObject().
                    get("followers_count").
                    getAsInt();
        } catch (NullPointerException nullex) {
            return 0;
        }
    }
}
