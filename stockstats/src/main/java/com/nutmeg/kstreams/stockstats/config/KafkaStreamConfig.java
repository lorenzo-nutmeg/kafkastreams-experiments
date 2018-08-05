package com.nutmeg.kstreams.stockstats.config;

import com.nutmeg.kstreams.stockstats.avro.TickerWindow;
import com.nutmeg.kstreams.stockstats.avro.Trade;
import com.nutmeg.kstreams.stockstats.avro.TradeStats;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

import java.util.*;

/**
 * Generate StreamsConfig Bean for configuring KafkaStreams
 * specific AVRO SerDe for generated types
 * and names of the topics used by the stream application
 */
@Configuration
public class KafkaStreamConfig {
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    @Value("#{'${kafka.bootstrap-servers}'.split(',')}")
    private List<String> bootstrapServers = new ArrayList<>();

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${kafka.streams.application}")
    private String streamApplication;

    @Value("${kafka.streams.source-topic}")
    private String sourceTopic;

    @Value("${kafka.streams.dest-topic}")
    private String destTopic;

    @Bean
    public StreamsConfig streamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamApplication);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        return new StreamsConfig(props);
    }

    public static class Topics {
        private final String sourceTopic;
        private final String destTopic;

        public Topics(String sourceTopic, String destTopic) {
            this.sourceTopic = sourceTopic;
            this.destTopic = destTopic;
        }

        public String sourceTopic() {
            return sourceTopic;
        }

        public String destTopic() {
            return destTopic;
        }
    }

    @Bean
    public Topics streamAppTopics() {
        return new Topics(sourceTopic, destTopic);
    }

    // Serdes

    private Map<String, Object> serdeProps() {
        return Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }

    @Bean
    public Serde<TradeStats> tradeStatsValueSerde() {
        Serde<TradeStats> serde = new SpecificAvroSerde<>();
        serde.configure(serdeProps(), false); // Set as VALUE Serde
        return serde;
    }

    @Bean
    public Serde<Trade> tradeValueSerde() {
        Serde<Trade> serde = new SpecificAvroSerde<>();
        serde.configure(serdeProps(), false); // Set as VALUE Serde
        return serde;
    }

    @Bean
    public Serde<TickerWindow> tickerWindowKeySerde() {
        Serde<TickerWindow> serde = new SpecificAvroSerde<>();
        serde.configure(serdeProps(), true); // Set as KEY Serde
        return serde;
    }


}
