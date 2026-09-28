package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.model.EventType;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;

/** 测试用内部事件构造器，receivedTime 默认取当前时间。 */
final class TestEvents {

    static final String SOURCE = "local-feed-1";

    private TestEvents() {
    }

    static NormalizedEvent quote(long offset, String symbol, long sequence, long eventTime,
                                 double bidPrice, double bidSize,
                                 double askPrice, double askSize) {
        return new NormalizedEvent(SOURCE, offset, symbol, sequence, EventType.QUOTE,
                eventTime, System.currentTimeMillis(),
                null, null, bidPrice, bidSize, askPrice, askSize);
    }

    static NormalizedEvent trade(long offset, String symbol, long sequence, long eventTime,
                                 double price, double size) {
        return new NormalizedEvent(SOURCE, offset, symbol, sequence, EventType.TRADE,
                eventTime, System.currentTimeMillis(),
                price, size, null, null, null, null);
    }

    static NormalizedEvent tradeFrom(String sourceId, long offset, String symbol,
                                     long sequence, long eventTime,
                                     double price, double size) {
        return new NormalizedEvent(sourceId, offset, symbol, sequence, EventType.TRADE,
                eventTime, System.currentTimeMillis(),
                price, size, null, null, null, null);
    }
}
