package com.nutmeg.kstreams.stockstats.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create KafkaAdmin bean
 */
@Configuration
public class KafkaAdminConfig {

    @Value("#{'${kafka.bootstrap-servers}'.split(',')}")
    private List<String> bootstrapServers = new ArrayList<>();

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(adminProps());
    }

    private Map<String, Object> adminProps() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return configs;
    }
}
