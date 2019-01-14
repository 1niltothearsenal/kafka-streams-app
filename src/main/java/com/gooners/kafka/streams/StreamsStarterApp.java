package com.gooners.kafka.streams;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsStarterApp.class);

    public static void main(String[] args) {

        //Use ctrl+d to duplicate the line
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"WordCount-Application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        // 1 - Stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        // 2 - Map values to lowercase can b alternatively written as:
        // .mapValues(String::toLowerCase)
        KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
                // 3 - flatmap values split by space
                .flatMapValues(lowerCaseTextLine -> Arrays.asList(lowerCaseTextLine.split(" ")))
                // 4 - select key to apply a key (we discard the old key)
                .selectKey((ignoredKey, word) -> word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurences
                .count("Counts");

        // 7 - in order to write the results back to kafka
        wordCounts.to(Serdes.String(),Serdes.Long(),"word-count-output");

        KafkaStreams streams  = new KafkaStreams(builder,config);
        streams.start();
        LOGGER.info("The topology of Kafka Streams is as follows {}",streams.toString());

        //Shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
