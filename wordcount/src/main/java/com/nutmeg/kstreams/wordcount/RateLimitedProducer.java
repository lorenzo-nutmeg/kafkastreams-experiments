package com.nutmeg.kstreams.wordcount;


import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Produces String messages to a topic, limiting the message rate to a max number of messages-per-second,
 * to avoid overrunning the Kafka cluster running on the same machine
 * (This is required only because we are running everything on the same machine)
 */
public class RateLimitedProducer {
    private static Logger LOG = LoggerFactory.getLogger(RateLimitedProducer.class);

    private final KafkaTemplate<String, String> template;

    private final String topicName;

    private final AtomicInteger countMessages = new AtomicInteger(0);

    // Limit the rate of messages per second,
    // considering we are producing, consuming and running the cluster on the same machine
   private final RateLimiter rateLimiter;

    public RateLimitedProducer(String topicName, KafkaTemplate<String, String> template, int maxMessagesPerSecond) {
        this.topicName = topicName;
        this.template = template;
        this.rateLimiter =  RateLimiter.create(maxMessagesPerSecond);
    }


    public void sendMessage(String message) {

        ListenableFuture<SendResult<String, String>> future = template.send(topicName, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                int count = countMessages.incrementAndGet();
                if ( count % 1000 == 0) LOG.debug("Sent '{}' to topic '{}'", result.getProducerRecord().value(), topicName);
            }

            @Override
            public void onFailure(Throwable ex) {
                LOG.warn("Error on sending message", ex);
            }
        });
    }
}
