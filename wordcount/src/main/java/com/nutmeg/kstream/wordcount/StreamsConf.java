package com.nutmeg.kstream.wordcount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create configuration Bean for Kafka Streams
 * re-using standard SpringBoot parameters like spring.kafka.bootstrap-servers
 */
@Configuration
@ConfigurationProperties("spring.kafka")
@EnableConfigurationProperties
public class StreamsConf {

    // The property AND the getter allow SpringBoot to automatically populate the List property from spring.kafka.boostrap-servers
    private List<String> bootstrapServers = new ArrayList<>();
    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    @Value("${streams.application}")
    private String streamApplication;

    @Bean
    public StreamsConfig streamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamApplication);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new StreamsConfig(props);
    }
}
