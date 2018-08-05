package com.nutmeg.kstreams.stockstats;

import com.google.common.util.concurrent.RateLimiter;
import com.nutmeg.kstreams.stockstats.avro.Trade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.nutmeg.kstreams.stockstats.Tickers.TICKERS;

public class TradeProducer {
    private static final Logger LOG = LoggerFactory.getLogger(TradeProducer.class);

    private static final int LOG_EVERY_N_MESSAGED = 100;

    private final KafkaTemplate<String, Trade> kafkaTemplate;


    private final AtomicInteger countTickers = new AtomicInteger(0);
    private final RateLimiter rateLimiter;

    private final Thread producerThread;

    public TradeProducer(KafkaTemplate<String, Trade> kafkaTemplate, int maxTradesPerSecond) {
        this.kafkaTemplate = kafkaTemplate;
        this.rateLimiter = RateLimiter.create(maxTradesPerSecond);
        this.producerThread = new Thread(tradeGenerator);
    }

    /**
     * Starts generating Trades on a separate thread
     */
    public void start() {
        producerThread.start();
    }

    /**
     * Stop the thread generating trades
     */
    public void stop() {
        producerThread.stop(); // This is not completely safe, but we don't care
    }

    private void sendTrade(Trade trade) {
        // For simplicity, limit send rate
        rateLimiter.acquire();

        final String ticker = trade.getTicker();
        final ListenableFuture<SendResult<String, Trade>> future = kafkaTemplate.sendDefault(ticker, trade);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Trade>>() {

            @Override
            public void onSuccess(SendResult<String, Trade> result) {
                if ( countTickers.incrementAndGet() % LOG_EVERY_N_MESSAGED == 0) LOG.debug("Sent {}", result.getProducerRecord().value() );
            }

            @Override
            public void onFailure(Throwable ex) {
                LOG.warn("Error sending message", ex);
            }
        });
    }


    // Arbitrary parameters for price fluctuation
    private static final int START_PRICE = 5000;
    private static final int MAX_PRICE_CHANGE = 5;


    private Runnable tradeGenerator = () -> {
        final Random random = new Random();
        long iter = 0;
        final Map<String, Integer> prices = new HashMap<>();

        // Start prices
        for (String ticker : TICKERS) {
            prices.put(ticker, START_PRICE);
        }

        // Generate trades until interrupted
        while (true) {
            iter++;
            for (String ticker : TICKERS) {
                double log = random.nextGaussian() * 0.25 + 1; // random var from lognormal dist with stddev = 0.25 and mean=1
                int size = random.nextInt(100);
                int price = prices.get(ticker);

                // fluctuate price sometimes
                if (iter % 10 == 0) {
                    price = price + random.nextInt(MAX_PRICE_CHANGE * 2) - MAX_PRICE_CHANGE;
                    prices.put(ticker, price);
                }

                final Trade trade = new Trade("ASK", ticker, (price + log), size); // Only generates "ASK"

                sendTrade(trade);
            }
        }
    };
}
