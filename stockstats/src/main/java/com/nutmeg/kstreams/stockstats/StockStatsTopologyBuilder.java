package com.nutmeg.kstreams.stockstats;

import com.nutmeg.kstreams.stockstats.avro.TickerWindow;
import com.nutmeg.kstreams.stockstats.avro.Trade;
import com.nutmeg.kstreams.stockstats.avro.TradeStats;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;


/**
 * Service for building the Kafka Stream topology for the Stock-stats application
 */
@Service
public class StockStatsTopologyBuilder {

    private final Serde<TradeStats> tradeStatsValueSerde;
    private final Serde<Trade> tradeValueSerde;
    private final Serde<TickerWindow> tickerWindowKeySerde;

    @Autowired
    public StockStatsTopologyBuilder(Serde<TradeStats> tradeStatsValueSerde, Serde<Trade> tradeValueSerde, Serde<TickerWindow> tickerWindowKeySerde) {
        this.tradeStatsValueSerde = tradeStatsValueSerde;
        this.tradeValueSerde = tradeValueSerde;
        this.tickerWindowKeySerde = tickerWindowKeySerde;
    }

    /**
     * Create a Topology
     * @param tradeTopic source topic containing Trades and tickers as key
     * @param statsTopic destination topic
     * @return a Topology instance
     */
    public Topology topology(String tradeTopic, String statsTopic) {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create the source from the trade topic, consuming the value with the specific Trade serde
//        KStream<String, Trade> source = builder.stream(tradeTopic, Consumed.with( Serdes.String(), tradeValueSerde ));
        // TODO Understand what's wrong in specifying input key serde here. Apparently it works only specifying serdes as default key serde
        KStream<String, Trade> source = builder.stream(tradeTopic);

        // Define the topology
        source.groupByKey() // Prepare for aggregating
                .windowedBy(aggregationWindow) // windowing for the following aggregation
                .aggregate(tradeStatsInit,      // Aggregate in a materialised windowed store
                        tradeAggregator,
                        Materialized.as(Stores.persistentWindowStore(
                                "trade-stats-store",
                                300_000L,
                                2,
                                aggregationWindow.size(),
                                false)))
                .toStream( (windowedTicker, tradeStats) ->
                        new TickerWindow( windowedTicker.key(), new DateTime( windowedTicker.window().start()))) // Turns the aggregation table into a stream with TickerWindow as key and TradeStats as value
                .mapValues( avgCalculator ) // Calculate average in TradeStats
                .to(statsTopic, Produced.with(tickerWindowKeySerde, tradeStatsValueSerde));

        return builder.build();
    }

    private static TimeWindows aggregationWindow = TimeWindows.of(5000).advanceBy(1000);


    private static Initializer<TradeStats> tradeStatsInit = () -> new TradeStats();

    /**
     * Aggregates Trades in TradeStats
     * Creates a new TradeStats aggregating an old TradeStats and a Trade
     * (remember TradeStats, as all AVRO-generated DTOs, is immutable)
     */
    private static Aggregator<String, Trade, TradeStats> tradeAggregator = (ticker, trade, tradeStats) -> {
         // Check trade has ticker and type
         if (trade.getType() == null || trade.getTicker() == null)
             throw new IllegalArgumentException("Invalid trade to aggregate: " + trade.toString());

         final String statsType = Optional.ofNullable(tradeStats.getType()).orElse(trade.getType());
         final String statsTicker = Optional.ofNullable(tradeStats.getTicker()).orElse(ticker);

         // Check we are aggregating for the same type and ticker
         if ( !ticker.equals(ticker) || !statsType.equals(trade.getType()))
             throw new IllegalArgumentException("Aggregating stats for trade type " + statsType + " and ticker " + ticker +
                     " but recieved trade of type " + trade.getType() +" and ticker " + trade.getTicker() );

         int countTrades = Optional.ofNullable(tradeStats.getCountTrades()).orElse(0);
         double minPrice = ( countTrades == 0 ) ? trade.getPrice() : NumberUtils.min(tradeStats.getMinPrice(), trade.getPrice() );
         double sumPrice = Optional.ofNullable(tradeStats.getSumPrice()).orElse(0.0) + trade.getPrice();
         Double avgPrice = tradeStats.getAvgPrice(); // Carries over avgPrice (it might be not calculated yet)

         countTrades++;

         return new TradeStats(statsType, statsTicker, countTrades, sumPrice, minPrice, avgPrice);
    };



    /**
     * Calculate the avg price in a TradeStats
     * (Creates a new TradeStats with the average)
     */
    private static ValueMapper<TradeStats, TradeStats> avgCalculator = (tradeStats) -> TradeStats.newBuilder(tradeStats)
                .setAvgPrice( tradeStats.getSumPrice() / tradeStats.getCountTrades() )
                .build();





}
