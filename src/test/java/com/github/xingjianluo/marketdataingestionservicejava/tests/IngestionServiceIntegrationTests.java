package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregate.MarketDataAggregator;
import com.github.xingjianluo.marketdataingestionservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.DuplicateReason;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import com.github.xingjianluo.marketdataingestionservicejava.model.SymbolAggregateView;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.InstrumentRegistry;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.MarketDataNormalizer;
import com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore;
import com.github.xingjianluo.marketdataingestionservicejava.replay.SourceSequenceTracker;
import com.github.xingjianluo.marketdataingestionservicejava.service.IngestionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 端到端：归一化 → 序号 → 聚合 → 持久化 的完整链路。
 * 使用真实对象（不 Mock），临时快照文件，验证乱序/重复/缺口/非法/迟到的最终结论。
 */
class IngestionServiceIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(IngestionServiceIntegrationTests.class);

    private Path stateFile;
    private IngestionService service;
    private ServiceProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        stateFile = Files.createTempFile("mdis-state-", ".json");
        Files.delete(stateFile); // store 负责创建
        properties = new ServiceProperties();
        properties.setStateFile(stateFile.toString());
        properties.setLateWindowMillis(2_000);
        buildService();
    }

    private void buildService() {
        var clock = java.time.Clock.systemUTC();
        var normalizer = new MarketDataNormalizer(new InstrumentRegistry(), clock);
        var tracker = new SourceSequenceTracker();
        var aggregator = new MarketDataAggregator(clock, properties);
        var store = new ReplayStateStore(new ObjectMapper(), properties);
        service = new IngestionService(normalizer, tracker, aggregator, store);
        service.restore();
    }

    private IngestionOutcome ingestAndLog(String label, RawMarketData raw) {
        var o = service.ingest(raw);
        log.info("[{}] 输入(source={},seq={},price={}) => {} reject={} dup={} flag={} | {}",
                label, raw.sourceId(), raw.sequence(), raw.price(), o.status(),
                o.rejectReason(), o.duplicateReason(), o.flag(), o.message());
        return o;
    }

    @Test
    @DisplayName("乱序+完全重复+同序号矛盾：结论可区分，聚合不重复")
    void outOfOrderDuplicatesAndContradiction() {
        RawMarketData r1 = trade("SIM-A", 1, 0, "100.00", "10");
        RawMarketData r3 = trade("SIM-A", 3, 200, "101.00", "5");
        RawMarketData r2 = trade("SIM-A", 2, 100, "100.50", "20");

        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("首条", r1).status());
        var gap = ingestAndLog("跳号3", r3);
        assertEquals(IngestionStatus.REJECTED, gap.status());
        assertEquals(RejectReason.SEQUENCE_GAP, gap.rejectReason());

        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("补齐2", r2).status());
        // seq=3 重传（之前缺口未占用）：接受
        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("3重传", r3).status());

        // seq=1 完全重复
        var dup = ingestAndLog("1完全重复", r1);
        assertEquals(IngestionStatus.DUPLICATE, dup.status());
        assertEquals(DuplicateReason.SAME_SEQUENCE_SAME_PAYLOAD, dup.duplicateReason());

        // seq=2 同序号不同内容：矛盾
        var contradiction = ingestAndLog("2矛盾",
                trade("SIM-A", 2, 100, "999.99", "20"));
        assertEquals(IngestionStatus.REJECTED, contradiction.status());
        assertEquals(RejectReason.CONTRADICTORY_RECORD, contradiction.rejectReason());

        SymbolAggregateView v = service.aggregateOf("600519.SH").orElseThrow();
        log.info("最终聚合 accepted={} dup={} rejected={} volume={} vwap={} last={}",
                v.acceptedCount(), v.duplicateCount(), v.rejectedCount(),
                v.totalVolume(), v.vwap(), v.lastTradePrice());
        assertEquals(3, v.acceptedCount());
        assertEquals(1, v.duplicateCount());
        // 缺口拒绝 + 矛盾拒绝
        assertEquals(2, v.rejectedCount());
        assertEquals(0, v.totalVolume().compareTo(new java.math.BigDecimal("35")));
        // 矛盾价格 999.99 绝不能进入最高/最新价
        assertEquals(0, v.highestPrice().compareTo(new java.math.BigDecimal("101.00")));
        assertEquals(0, v.lastTradePrice().compareTo(new java.math.BigDecimal("101.00")));
    }

    @Test
    @DisplayName("非法批次：每条都被按原因拒绝，绝不静默丢弃")
    void invalidBatchRejectedWithReasons() {
        List<RawMarketData> batch = new com.github.xingjianluo.marketdataingestionservicejava
                .source.LocalMarketDataSource().batch("invalid").records();
        int rejected = 0;
        for (RawMarketData raw : batch) {
            var o = ingestAndLog("非法批次", raw);
            assertEquals(IngestionStatus.REJECTED, o.status(), "非法输入必须被拒绝: " + raw);
            assertNotNull(o.rejectReason(), "拒绝原因必须非空: " + raw);
            rejected++;
        }
        assertEquals(batch.size(), rejected);
    }

    @Test
    @DisplayName("窗口外迟到被 LATE_OUT_OF_WINDOW 拒绝且不混入聚合")
    void lateOutOfWindowRejected() {
        var a = ingestAndLog("主序1", trade("SIM-A", 1, 10_000, "110", "10"));
        assertEquals(IngestionStatus.ACCEPTED, a.status());
        var lateIn = ingestAndLog("窗口内迟到", trade("SIM-A", 2, 9_000, "109", "10"));
        assertEquals(IngestionStatus.ACCEPTED, lateIn.status());
        assertEquals(com.github.xingjianluo.marketdataingestionservicejava.model.AcceptedFlag
                .ACCEPTED_LATE_IN_WINDOW, lateIn.flag());
        var lateOut = ingestAndLog("窗口外迟到", trade("SIM-A", 3, 5_000, "101", "10"));
        assertEquals(IngestionStatus.REJECTED, lateOut.status());
        assertEquals(RejectReason.LATE_OUT_OF_WINDOW, lateOut.rejectReason());

        SymbolAggregateView v = service.aggregateOf("600519.SH").orElseThrow();
        assertEquals(2, v.acceptedCount());
        assertEquals(0, v.totalVolume().compareTo(new java.math.BigDecimal("20")));
    }

    @Test
    @DisplayName("同一批数据跑两遍：第二遍全部 DUPLICATE，聚合数值不变（确定性）")
    void repeatedBatchIsIdempotent() {
        var source = new com.github.xingjianluo.marketdataingestionservicejava
                .source.LocalMarketDataSource();
        List<RawMarketData> batch = source.batch("ordered").records();
        batch.forEach(r -> ingestAndLog("第一遍", r));
        SymbolAggregateView first = service.aggregateOf("600519.SH").orElseThrow();

        int duplicates = 0;
        for (RawMarketData r : batch) {
            var o = ingestAndLog("第二遍", r);
            if (o.status() == IngestionStatus.DUPLICATE) {
                duplicates++;
            }
        }
        SymbolAggregateView second = service.aggregateOf("600519.SH").orElseThrow();
        log.info("第二遍重复数={}, 两遍 volume={}/{}, vwap={}/{}", duplicates,
                first.totalVolume(), second.totalVolume(), first.vwap(), second.vwap());
        assertEquals(batch.size(), duplicates);
        assertEquals(first.acceptedCount(), second.acceptedCount());
        assertEquals(0, first.totalVolume().compareTo(second.totalVolume()));
        assertEquals(first.vwap(), second.vwap());
    }

    private RawMarketData trade(String source, long seq, long offsetMs,
                                String price, String size) {
        String ts = java.time.Instant.parse(
                com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketDataSource
                        .BASE_TIME).plusMillis(offsetMs).toString();
        return new RawMarketData(source, seq, "TRADE", "600519.SH", ts, price, size,
                seq % 2 == 0 ? "SELL" : "BUY");
    }
}
