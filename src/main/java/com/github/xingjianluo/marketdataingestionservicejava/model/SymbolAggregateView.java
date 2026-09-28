package com.github.xingjianluo.marketdataingestionservicejava.model;

import java.math.BigDecimal;
import java.time.Instant;

/** 单个标的的聚合结果只读视图。 */
public record SymbolAggregateView(
        String symbol,
        long acceptedCount,
        long duplicateCount,
        long rejectedCount,
        long tradeCount,
        BigDecimal totalVolume,
        BigDecimal lastTradePrice,
        BigDecimal highestPrice,
        BigDecimal lowestPrice,
        BigDecimal vwap,
        BigDecimal latestBid,
        BigDecimal latestAsk,
        Instant lastEventTime,
        Instant lastIngestedAt,
        Long lastSequence,
        String lastSourceId
) {
}
