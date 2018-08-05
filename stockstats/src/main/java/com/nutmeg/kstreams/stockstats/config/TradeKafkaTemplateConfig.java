package com.nutmeg.kstreams.stockstats.config;

import com.nutmeg.kstreams.stockstats.avro.Trade;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class TradeKafkaTemplateConfig {
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    @Value("#{'${kafka.bootstrap-servers}'.split(',')}")
    private List<String> bootstrapServers = new ArrayList<>();

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${kafka.producer.trade.acks}")
    private String acks;

    @Value("${kafka.producer.trade.retries}")
    private int retries;

    @Value("${kafka.producer.trade.batch-size}")
    private int batchSize;

    @Value("${kafka.producer.trade.linger-ms}")
    private int lingerMs;

    @Value("${kafka.producer.trade.retry-backoff-ms}")
    private int retryBackoffMs;

    @Value("${kafka.producer.trade.topic}")
    private String topic;


    private Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        return props;
    }

    /**
     * Create KafkaTemplate Bean for producing Trades
     */
    @Bean
    public KafkaTemplate<String, Trade> tradeKafkaTemplate() {
        ProducerFactory<String, Trade> producerFactory = new DefaultKafkaProducerFactory<>(producerProps());
        KafkaTemplate<String, Trade> template = new KafkaTemplate(producerFactory);
        template.setDefaultTopic(topic);
        return template;
    }
}
