package com.nutmeg.kstreams.stockstats;

import com.nutmeg.kstreams.stockstats.avro.Trade;
import com.nutmeg.kstreams.stockstats.config.KafkaStreamConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

@SpringBootApplication
public class StockStatsApplication implements CommandLineRunner {
    private static Logger LOG = LoggerFactory.getLogger(StockStatsApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(StockStatsApplication.class, args).close();
    }

    @Autowired
    private KafkaTemplate<String, Trade> tradeKafkaTemplate;

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private StockStatsTopologyBuilder topologyBuilder;

    @Autowired
    private StreamsConfig streamsConfig;

    @Autowired
    private KafkaStreamConfig.Topics streamAppTopics;

    @Autowired
    private KafkaMessageListenerContainer statsLoggerMessageListenerContainer;

    @Override
    public void run(String... args) throws Exception {

        // Generate topics
        final TopicAdmin topicAdmin = new TopicAdmin(kafkaAdmin);
        topicAdmin.createTopic(streamAppTopics.sourceTopic(), 3, (short)2);
        topicAdmin.createTopic(streamAppTopics.destTopic(), 3, (short)2);

        // Better waiting few secs to give time to leader election
        Thread.sleep(3000);

        // Create the topology
        Topology topology = topologyBuilder.topology(streamAppTopics.sourceTopic(), streamAppTopics.destTopic());

        // Run the stream application
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();

        // Starts the logging consumer
        statsLoggerMessageListenerContainer.start();

        // Start generating Trades
        final TradeProducer tradeProducer = new TradeProducer(tradeKafkaTemplate, 50);
        tradeProducer.start();

        // Stop the application after a while
        // Shudown everything
        Thread.sleep(120_000L);
        streams.close();
        tradeProducer.stop();
        statsLoggerMessageListenerContainer.stop();
    }
}
