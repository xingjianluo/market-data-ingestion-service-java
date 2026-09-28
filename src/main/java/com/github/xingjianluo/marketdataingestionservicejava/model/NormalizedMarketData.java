package com.github.xingjianluo.marketdataingestionservicejava.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 归一化后的统一内部模型。只有通过字段/时间戳/标的校验的报文才会得到该对象。
 */
public record NormalizedMarketData(
        String sourceId,
        long sequence,
        MarketDataType type,
        String symbol,
        Instant eventTime,
        BigDecimal price,
        BigDecimal size,
        String side
) {
}
