package com.nutmeg.kstreams.stockstats.config;

import com.nutmeg.kstreams.stockstats.TradeStatsLoggingListener;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

    @Value("#{'${kafka.bootstrap-servers}'.split(',')}")
    private List<String> bootstrapServers = new ArrayList<>();

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;


    @Value("${kafka.consumer.trade-stats.topic}")
    private String topic;

    @Value("${kafka.consumer.trade-stats.group-id}")
    private String groupId;

    @Value("${kafka.consumer.trade-stats.max-poll-interval-ms}")
    private int maxPoolIntervalMs;

    @Value("${kafka.consumer.trade-stats.enable-auto-commit}")
    private boolean enableAutoCommit;

    @Value("${kafka.consumer.trade-stats.auto-commit-interval-ms}")
    private int autoCommitIntervalMs;

    @Value("${kafka.consumer.trade-stats.auto-offset-reset}")
    private String autoOffsetReset;


    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPoolIntervalMs);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }

    /**
     * Create the listener container for consuming stats messages
     * without starting it
     */
    @Bean
    public KafkaMessageListenerContainer statsLoggerMessageListenerContainer(){
        ContainerProperties containerProperties = new ContainerProperties(topic);
        TradeStatsLoggingListener listener = new TradeStatsLoggingListener();
        containerProperties.setMessageListener(listener);
        KafkaMessageListenerContainer kafkaMessageListenerContainer = new KafkaMessageListenerContainer(consumerFactory(), containerProperties);
        kafkaMessageListenerContainer.setBeanName("stats-logger");
//        kafkaMessageListenerContainer.start();
        return kafkaMessageListenerContainer;
    }

    private ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }
}
