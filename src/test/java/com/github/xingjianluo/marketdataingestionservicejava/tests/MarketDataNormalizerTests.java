package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.InstrumentRegistry;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.MarketDataNormalizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 归一化层单测：覆盖正常接受与全部拒绝原因，日志打印输入与判定依据。
 */
class MarketDataNormalizerTests {

    private static final Logger log = LoggerFactory.getLogger(MarketDataNormalizerTests.class);

    /** 固定“现在”，未来时间戳校验可重复。 */
    private static final Instant NOW = Instant.parse("2026-09-28T02:00:00Z");

    private MarketDataNormalizer normalizer;

    @BeforeEach
    void setUp() {
        normalizer = new MarketDataNormalizer(new InstrumentRegistry(),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private void assertRejected(RawMarketData raw, RejectReason expected) {
        var outcome = normalizer.normalize(raw);
        log.info("输入={} => status={} reason={} 依据={}",
                raw, outcome.status(), outcome.rejectReason(), outcome.message());
        assertEquals(IngestionStatus.REJECTED, outcome.status());
        assertEquals(expected, outcome.rejectReason(),
                "拒绝原因应可区分，输入=" + raw);
    }

    private RawMarketData base(String source, Long seq, String type, String symbol,
                               String ts, String price, String size, String side) {
        return new RawMarketData(source, seq, type, symbol, ts, price, size, side);
    }

    @Test
    @DisplayName("合法成交与报价被接受并归一化")
    void acceptsValidTradeAndQuote() {
        RawMarketData trade = base("S", 1L, "TRADE", "600519.SH",
                NOW.toString(), "100.25", "10", "BUY");
        var o1 = normalizer.normalize(trade);
        log.info("输入={} => status={} flag={} 依据={}", trade, o1.status(), o1.flag(), o1.message());
        assertEquals(IngestionStatus.ACCEPTED, o1.status());
        assertNotNull(o1.normalized());
        assertEquals(0, new java.math.BigDecimal("100.25")
                .compareTo(o1.normalized().price()));

        RawMarketData quote = base("S", 2L, "QUOTE", "AAPL.US",
                NOW.toString(), "175.5", "3", "ASK");
        var o2 = normalizer.normalize(quote);
        log.info("输入={} => status={} 依据={}", quote, o2.status(), o2.message());
        assertEquals(IngestionStatus.ACCEPTED, o2.status());
    }

    @Test
    @DisplayName("字段缺失：源/序号/类型/标的/方向分别给出 MISSING_FIELD")
    void rejectsMissingFields() {
        assertRejected(base("  ", 1L, "TRADE", "600519.SH", NOW.toString(), "1", "1", "BUY"),
                RejectReason.MISSING_FIELD);
        assertRejected(base("S", null, "TRADE", "600519.SH", NOW.toString(), "1", "1", "BUY"),
                RejectReason.MISSING_FIELD);
        assertRejected(base("S", 0L, "TRADE", "600519.SH", NOW.toString(), "1", "1", "BUY"),
                RejectReason.MISSING_FIELD);
        assertRejected(base("S", 1L, "  ", "600519.SH", NOW.toString(), "1", "1", "BUY"),
                RejectReason.MISSING_FIELD);
        assertRejected(base("S", 1L, "TRADE", "  ", NOW.toString(), "1", "1", "BUY"),
                RejectReason.MISSING_FIELD);
        assertRejected(base("S", 1L, "TRADE", "600519.SH", NOW.toString(), "1", "1", null),
                RejectReason.MISSING_FIELD);
    }

    @Test
    @DisplayName("未知类型 INVALID_PAYLOAD，类型与方向矛盾 CONTRADICTORY_RECORD")
    void rejectsInvalidTypeAndContradictorySide() {
        assertRejected(base("S", 1L, "FILL", "600519.SH", NOW.toString(), "1", "1", "BUY"),
                RejectReason.INVALID_PAYLOAD);
        assertRejected(base("S", 2L, "TRADE", "600519.SH", NOW.toString(), "1", "1", "BID"),
                RejectReason.CONTRADICTORY_RECORD);
        assertRejected(base("S", 3L, "QUOTE", "600519.SH", NOW.toString(), "1", "1", "BUY"),
                RejectReason.CONTRADICTORY_RECORD);
    }

    @Test
    @DisplayName("未知标的 UNKNOWN_SYMBOL")
    void rejectsUnknownSymbol() {
        assertRejected(base("S", 1L, "TRADE", "404.NOPE", NOW.toString(), "1", "1", "BUY"),
                RejectReason.UNKNOWN_SYMBOL);
    }

    @Test
    @DisplayName("时间戳非法：不可解析/过早/超未来，均为 INVALID_TIMESTAMP")
    void rejectsInvalidTimestamps() {
        assertRejected(base("S", 1L, "TRADE", "600519.SH", "garbage", "1", "1", "BUY"),
                RejectReason.INVALID_TIMESTAMP);
        assertRejected(base("S", 2L, "TRADE", "600519.SH",
                "1990-01-01T00:00:00Z", "1", "1", "BUY"), RejectReason.INVALID_TIMESTAMP);
        assertRejected(base("S", 3L, "TRADE", "600519.SH",
                NOW.plusSeconds(600).toString(), "1", "1", "BUY"), RejectReason.INVALID_TIMESTAMP);
        // 容忍边界内的轻微未来时间应接受
        var nearFuture = normalizer.normalize(base("S", 4L, "TRADE", "600519.SH",
                NOW.plusSeconds(30).toString(), "1", "1", "BUY"));
        log.info("输入轻微未来时间 => status={} 依据={}", nearFuture.status(), nearFuture.message());
        assertEquals(IngestionStatus.ACCEPTED, nearFuture.status());
    }

    @Test
    @DisplayName("数值非法：不可解析/零/负数，均为 INVALID_NUMBER")
    void rejectsInvalidNumbers() {
        assertRejected(base("S", 1L, "TRADE", "600519.SH", NOW.toString(), "abc", "1", "BUY"),
                RejectReason.INVALID_NUMBER);
        assertRejected(base("S", 2L, "TRADE", "600519.SH", NOW.toString(), "0", "1", "BUY"),
                RejectReason.INVALID_NUMBER);
        assertRejected(base("S", 3L, "TRADE", "600519.SH", NOW.toString(), "100", "-5", "BUY"),
                RejectReason.INVALID_NUMBER);
        assertRejected(base("S", 4L, "TRADE", "600519.SH", NOW.toString(), "", "1", "BUY"),
                RejectReason.INVALID_NUMBER);
    }

    @Test
    @DisplayName("同一报文重复归一化结论完全一致（确定性）")
    void deterministicRepeatedNormalization() {
        RawMarketData raw = base("S", 1L, "TRADE", "600519.SH",
                NOW.toString(), "100.25", "10", "BUY");
        var first = normalizer.normalize(raw);
        var second = normalizer.normalize(raw);
        log.info("两次归一化: {} / {}", first.message(), second.message());
        assertEquals(first.status(), second.status());
        assertEquals(first.message(), second.message());
    }
}
