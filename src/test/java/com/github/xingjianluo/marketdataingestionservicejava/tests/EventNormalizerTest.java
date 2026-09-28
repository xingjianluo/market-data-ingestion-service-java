package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties;
import com.github.xingjianluo.marketdataingestionservicejava.ingest.EventNormalizer;
import com.github.xingjianluo.marketdataingestionservicejava.instrument.InstrumentRegistry;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 归一化校验：字段缺失、非法时间戳、未知标的、内在矛盾必须按原因区分拒绝。
 */
class EventNormalizerTest {

    private static final Logger log = LoggerFactory.getLogger(EventNormalizerTest.class);

    private final long now = System.currentTimeMillis();
    private final EventNormalizer normalizer =
            new EventNormalizer(new InstrumentRegistry(new MarketDataProperties()));

    @Test
    void validTradeAndQuoteAreAccepted() {
        RawMarketEvent trade = new RawMarketEvent("s1", 1L, "BTC-USD", 1L,
                "trade", now, 100.2, 2.0, null, null, null, null);
        RawMarketEvent quote = new RawMarketEvent("s1", 2L, "ETH-USD", 1L,
                "QUOTE", now, null, null, 3000.0, 1.0, 3000.5, 1.0);

        var tradeResult = normalizer.normalize(trade);
        var quoteResult = normalizer.normalize(quote);

        log.info("输入={} 判定=valid:{}, 归一类型={}", trade, tradeResult.valid(),
                tradeResult.valid() ? tradeResult.event().type() : tradeResult.reason());
        log.info("输入={} 判定=valid:{}, 归一类型={}", quote, quoteResult.valid(),
                quoteResult.valid() ? quoteResult.event().type() : quoteResult.reason());

        assertTrue(tradeResult.valid());
        assertTrue(quoteResult.valid());
        // 类型大小写不敏感
        assertEquals(com.github.xingjianluo.marketdataingestionservicejava.model.EventType.TRADE,
                tradeResult.event().type());
    }

    @Test
    void invalidInputsAreRejectedWithDistinctReasons() {
        record Case(String name, RawMarketEvent input, RejectReason expected) {
        }
        List<Case> cases = List.of(
                new Case("缺源", new RawMarketEvent(null, 1L, "BTC-USD", 1L,
                        "QUOTE", now, null, null, 100.0, 1.0, 100.5, 1.0),
                        RejectReason.MISSING_SOURCE_ID),
                new Case("缺标的", new RawMarketEvent("s1", 1L, " ", 1L,
                        "QUOTE", now, null, null, 100.0, 1.0, 100.5, 1.0),
                        RejectReason.MISSING_SYMBOL),
                new Case("缺位点", new RawMarketEvent("s1", null, "BTC-USD", 1L,
                        "QUOTE", now, null, null, 100.0, 1.0, 100.5, 1.0),
                        RejectReason.MISSING_OFFSET),
                new Case("缺序号", new RawMarketEvent("s1", 1L, "BTC-USD", null,
                        "QUOTE", now, null, null, 100.0, 1.0, 100.5, 1.0),
                        RejectReason.MISSING_SEQUENCE),
                new Case("缺时间", new RawMarketEvent("s1", 1L, "BTC-USD", 1L,
                        "QUOTE", null, null, null, 100.0, 1.0, 100.5, 1.0),
                        RejectReason.MISSING_EVENT_TIME),
                new Case("类型未知", new RawMarketEvent("s1", 1L, "BTC-USD", 1L,
                        "WAT", now, null, null, null, null, null, null),
                        RejectReason.UNSUPPORTED_TYPE),
                new Case("时间戳为负", new RawMarketEvent("s1", 2L, "BTC-USD", 2L,
                        "TRADE", -3L, 100.0, 1.0, null, null, null, null),
                        RejectReason.INVALID_TIMESTAMP),
                new Case("时间戳在遥远未来", new RawMarketEvent("s1", 3L, "BTC-USD", 3L,
                        "TRADE", now + 365L * 24 * 3600 * 1000, 100.0, 1.0,
                        null, null, null, null),
                        RejectReason.INVALID_TIMESTAMP),
                new Case("未知标的", new RawMarketEvent("s1", 4L, "FOO-USD", 4L,
                        "TRADE", now, 0.5, 100.0, null, null, null, null),
                        RejectReason.UNKNOWN_SYMBOL),
                new Case("成交缺字段", new RawMarketEvent("s1", 5L, "BTC-USD", 5L,
                        "TRADE", now, null, 2.0, null, null, null, null),
                        RejectReason.MISSING_TRADE_FIELDS),
                new Case("报价缺字段", new RawMarketEvent("s1", 6L, "BTC-USD", 6L,
                        "QUOTE", now, null, null, 100.0, null, 100.5, 1.0),
                        RejectReason.MISSING_QUOTE_FIELDS),
                new Case("价格非法", new RawMarketEvent("s1", 7L, "BTC-USD", 7L,
                        "TRADE", now, 0d, 2.0, null, null, null, null),
                        RejectReason.INVALID_PRICE),
                new Case("数量为负", new RawMarketEvent("s1", 8L, "BTC-USD", 8L,
                        "TRADE", now, 100.0, -1.0, null, null, null, null),
                        RejectReason.NEGATIVE_SIZE),
                new Case("买价高于卖价", new RawMarketEvent("s1", 9L, "BTC-USD", 9L,
                        "QUOTE", now, null, null, 101.0, 1.0, 100.0, 1.0),
                        RejectReason.INCONSISTENT_QUOTE)
        );

        for (Case c : cases) {
            var result = normalizer.normalize(c.input());
            log.info("[{}] 输入={} => 判定valid={}, 原因={}", c.name(), c.input(),
                    result.valid(), result.reason());
            assertFalse(result.valid(), c.name() + " 应被拒绝");
            assertEquals(c.expected(), result.reason(),
                    c.name() + " 的拒绝原因应可区分");
        }
        assertTrue(cases.size() >= 14, "应覆盖全部边界分支");
    }
}
