package com.nutmeg.kstreams.stockstats;

import com.nutmeg.kstreams.stockstats.avro.Trade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

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

    @Override
    public void run(String... args) throws Exception {

        // Generate topics
        final TopicAdmin topicAdmin = new TopicAdmin(kafkaAdmin);
        topicAdmin.createTopic("stockstats.trades", 3, (short)2); // FIXME get topic name by configuration

        // Start generating Trades
        final TradeProducer tradeProducer = new TradeProducer(tradeKafkaTemplate, 50);
        tradeProducer.start();
    }
}
