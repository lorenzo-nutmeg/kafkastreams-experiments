package com.nutmeg.kstreams.stockstats;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * Utility to programmatically create topics, if the do not exist
 */
public class TopicAdmin {
    private static Logger LOG = LoggerFactory.getLogger(TopicAdmin.class);

    private final  KafkaAdmin admin;

    public TopicAdmin(KafkaAdmin admin) {
        this.admin = admin;
    }

    /**
     * Create multiple partitions, if the do not exist
     * all with the with same n. of partitions and replicas
     * @param topics topic names
     * @param partitions n of partitions
     * @param replicas replication factor
     */
    public void createTopics(Collection<String> topics, int partitions, short replicas) {
        // TODO it would be nice to run them in parallel
        topics.stream()
                .forEach( topic ->  createTopic(topic, partitions, replicas ));
    }

    /**
     * Create a topic if it does not exist
     * all with the with same n. of partitions and replicas
     * @param topic topic name
     * @param partitions n of partitions
     * @param replicas replication factor
     */
    public void createTopic( String topic, int partitions, short replicas) {
        LOG.debug("Creating topic '{}'. {} partitions, {} replicas", topic, partitions, replicas);

        try (final AdminClient adminClient = AdminClient.create(admin.getConfig())) {
            try {
                final NewTopic newTopic = new NewTopic(topic, partitions, replicas);
                // Create topic, which is async call.
                final CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));

                // Since the call is Async, Lets wait for it to complete.
                createTopicsResult.values().get(topic).get();
                LOG.info("Topic '{}' created", topic);
            } catch (InterruptedException | ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new RuntimeException(e.getMessage(), e);
                } else {
                    LOG.debug("Topic '{}' already exist", topic);
                }
            }
        }
    }
}
