package com.github.xingjianluo.marketdataingestionservicejava.aggregation;

import com.github.xingjianluo.marketdataingestionservicejava.model.AggregateSnapshot;
import com.github.xingjianluo.marketdataingestionservicejava.model.EventType;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;

/**
 * 单一标的的可变聚合状态。
 *
 * <p>不是线程安全的：所有访问都必须经由 {@link SymbolAggregator} 的内置锁串行化，
 * 从而保证同一时刻不会有重复计价或数值错乱。
 */
class SymbolAggregate {

    private final String symbol;

    /** 最近一次已应用事件的序号；null 表示尚无任何事件。 */
    private Long lastSequence;
    /** 已应用过的序号 -> 内容签名，用于重复/矛盾判定。 */
    private final java.util.Map<Long, String> seenSignatures = new java.util.HashMap<>();
    /** 已应用过的序号 -> 源位点，用于位点回退判定。 */
    private final java.util.Map<Long, Long> seenOffsets = new java.util.HashMap<>();

    private long watermark = Long.MIN_VALUE;

    private Long lastTradeTime;
    private Double lastTradePrice;
    private double totalVolume;
    private long tradeCount;
    private double totalTurnover;
    private Double highPrice;
    private Double lowPrice;

    private Long lastQuoteTime;
    private Double bestBidPrice;
    private Double bestBidSize;
    private Double bestAskPrice;
    private Double bestAskSize;

    private long acceptedCount;
    private long duplicateCount;
    private long rejectedCount;
    private long lateRejectedCount;

    SymbolAggregate(String symbol) {
        this.symbol = symbol;
    }

    String symbol() {
        return symbol;
    }

    Long lastSequence() {
        return lastSequence;
    }

    long watermark() {
        return watermark;
    }

    String signatureAt(long sequence) {
        return seenSignatures.get(sequence);
    }

    Long offsetAt(long sequence) {
        return seenOffsets.get(sequence);
    }

    void incrementDuplicate() {
        duplicateCount++;
    }

    void incrementRejected(boolean late) {
        rejectedCount++;
        if (late) {
            lateRejectedCount++;
        }
    }

    /** 应用一条已通过所有检查的事件（恢复重放走同一路径，保证结果一致）。 */
    void apply(NormalizedEvent e) {
        seenSignatures.put(e.sequence(), e.contentSignature());
        seenOffsets.put(e.sequence(), e.offset());
        lastSequence = e.sequence();
        if (e.eventTime() > watermark) {
            watermark = e.eventTime();
        }
        acceptedCount++;

        if (e.type() == EventType.TRADE) {
            lastTradeTime = e.eventTime();
            lastTradePrice = e.price();
            totalVolume += e.size();
            totalTurnover += e.price() * e.size();
            tradeCount++;
            highPrice = highPrice == null ? e.price() : Math.max(highPrice, e.price());
            lowPrice = lowPrice == null ? e.price() : Math.min(lowPrice, e.price());
        } else {
            lastQuoteTime = e.eventTime();
            bestBidPrice = e.bidPrice();
            bestBidSize = e.bidSize();
            bestAskPrice = e.askPrice();
            bestAskSize = e.askSize();
        }
    }

    AggregateSnapshot snapshot() {
        Double vwap = tradeCount == 0 ? null : totalTurnover / totalVolume;
        return new AggregateSnapshot(
                symbol,
                lastSequence,
                lastTradeTime,
                lastTradePrice,
                tradeCount == 0 ? null : totalVolume,
                tradeCount == 0 ? null : tradeCount,
                highPrice,
                lowPrice,
                vwap,
                lastQuoteTime,
                bestBidPrice,
                bestBidSize,
                bestAskPrice,
                bestAskSize,
                watermark == Long.MIN_VALUE ? null : watermark,
                acceptedCount,
                duplicateCount,
                rejectedCount,
                lateRejectedCount
        );
    }
}
