package com.nutmeg.kstream.wordcount;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Read all lines of a book ("War and Peace"!) from a file and send them to a topic.
 * Use a Kafka Stream topology to split words, filter out articles and count word occurrences,
 * sending word counts to a final topic.
 * Single words are sent to an intermediate topic. This is not strictly required but useful to observe what's going on.
 *
 * Topics are created programmatically by the application, if the do not exist.
 *
 * Three listener reads and log the three topics, book lines, words and word counts, to show what's happening.
 */
@SpringBootApplication
public class WordCountApplication implements CommandLineRunner {
    private static Logger LOG = LoggerFactory.getLogger(WordCountApplication.class);

    static final String TOPIC_BOOK_LINES = "wordcount.book-lines";
    static final String TOPIC_BOOK_WORDS = "wordcount.book-words";
    static final String TOPIC_BOOK_WORDCOUNTS = "wordcount.book-wordcounts";

    private static final int PARTITIONS = 3;
    private static final short REPLICAS = 2;


    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    private StreamsConfig streamConfig;

    @Autowired
    private  TopicAdmin topicAdmin;

    public static void main(String[] args) {
        SpringApplication.run(WordCountApplication.class, args).close();
    }

    @Override
    public void run(String... args) throws Exception {

        // Create all topics
        topicAdmin.createTopics( Arrays.asList(TOPIC_BOOK_LINES, TOPIC_BOOK_WORDS, TOPIC_BOOK_WORDCOUNTS), PARTITIONS, REPLICAS);

        // Build the topology
        Topology topology = WordCountTopology.topology(TOPIC_BOOK_LINES, TOPIC_BOOK_WORDS, TOPIC_BOOK_WORDCOUNTS);

        // Run the stream app
        KafkaStreams streams = new KafkaStreams(topology, streamConfig);
        streams.start();

        // Give time to KStreams for being up and running
        Thread.sleep(2000L);

        // Read the book into a topic
        final RateLimitedProducer producer = new RateLimitedProducer(TOPIC_BOOK_LINES, template, 200);
        try ( final Stream<String> lines = BookReader.bookLines("war_and_peace.txt") ) {
            lines.forEach(producer::sendMessage);
        }


        // Stop the application after a while
        Thread.sleep(30_000L);
        streams.close();
    }



    // The consumers below are just for see what's passing through topics
    // They are not required for processing

    // TODO Consumers may start before topics have been created, causing some WARN messages

    // I'm counting messages just to log every
    private static final AtomicLong countLines = new AtomicLong(0);
    private static final AtomicLong countWords = new AtomicLong(0);
    private static final AtomicLong countWordCounts = new AtomicLong(0);

    @KafkaListener(topics = TOPIC_BOOK_LINES, groupId = "lines.logger")
    public void bookLines(ConsumerRecord<?, String> record) {
        String bookLine = record.value().toString();

        long count = countLines.getAndIncrement();
        if (count %  500  == 0) LOG.debug("Line #{}: '{}'", count, bookLine);
    }


    @KafkaListener(topics = TOPIC_BOOK_WORDS, groupId = "words.logger")
    public void bookWords(ConsumerRecord<?, String> record) {
        String word = record.value().toString();

        long count = countWords.getAndIncrement();
        if (count % 500 == 0 ) LOG.debug("Word #{}: '{}'", count, word);
    }


    @KafkaListener(topics = TOPIC_BOOK_WORDCOUNTS, groupId = "wordcount.logger")
    public void wordCounts(ConsumerRecord<?, ?> record) {
        String word = record.key().toString();
        String wordCount = record.value().toString();

        long count = countWordCounts.getAndIncrement();
        if (count % 100 == 0 ) LOG.debug("WordCount: '{}' {} times", word, wordCount);
    }


}
