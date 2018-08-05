package com.nutmeg.kstreams.stockstats;

import com.nutmeg.kstreams.stockstats.avro.TickerWindow;
import com.nutmeg.kstreams.stockstats.avro.TradeStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Listener consuming the stats topic and logging the content
 */
public class TradeStatsLoggingListener implements MessageListener<TickerWindow, TradeStats> {
    private static Logger LOG = LoggerFactory.getLogger(TradeStatsLoggingListener.class);

    private AtomicLong count = new AtomicLong(0);

    @Override
    public void onMessage(ConsumerRecord<TickerWindow, TradeStats> record) {
        TickerWindow tickerWindow = record.key();
        TradeStats tradeStats = record.value();
        LOG.debug("Stats: {} = {}", tickerWindow, tradeStats);
    }
}
