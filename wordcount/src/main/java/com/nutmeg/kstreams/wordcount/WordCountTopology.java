package com.nutmeg.kstreams.wordcount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Defines the Kafka Stream topology for the word count application
 */
public class WordCountTopology {

    public static Topology topology(String sourceTopic, String wordTopic, String destinationTopic) {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(sourceTopic);

        final Pattern splitWordsPattern = Pattern.compile("\\W+");

        source.flatMapValues( line -> Arrays.asList(splitWordsPattern.split(line.toLowerCase())) )
                .map((_key,word) -> new KeyValue<>(word, word)) // drop the key in the source topic
                .filter((_key, word) -> isNotEmpty(word)) // Filter out empty words
                .filter((_key, word) -> isNotAnArticle(word)) // Filter out articles
                .through(wordTopic, Produced.with(Serdes.String(), Serdes.String())) // Send words through an intermediate topic (not required)
                .groupByKey() // Create a KGroupedStream, group by word
                .count(Materialized.as("CountStore")) // Count the elements in each group -> a stream of <String,Long> (word, count)
                .mapValues(count -> Long.toString(count)) // Convert count to String (human friendly)
                .toStream()
                .to(destinationTopic, Produced.with(Serdes.String(), Serdes.String())); // Send words (key) and counts (value) to the destination topic

        return builder.build();
    }

    private static boolean isNotAnArticle(String word) {
        return !word.equals("the") &&
                !word.equals("a") &&
                !word.equals("an");
    }
}
