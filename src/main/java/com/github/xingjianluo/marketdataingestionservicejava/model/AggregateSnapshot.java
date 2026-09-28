package com.github.xingjianluo.marketdataingestionservicejava.model;

/** 某一标的当前聚合快照。 */
public record AggregateSnapshot(
        String symbol,
        Long lastSequence,
        Long lastTradeTime,
        Double lastTradePrice,
        Double totalVolume,
        Long tradeCount,
        Double highPrice,
        Double lowPrice,
        Double vwap,
        Long lastQuoteTime,
        Double bestBidPrice,
        Double bestBidSize,
        Double bestAskPrice,
        Double bestAskSize,
        Long watermark,
        long acceptedCount,
        long duplicateCount,
        long rejectedCount,
        long lateRejectedCount
) {
}
