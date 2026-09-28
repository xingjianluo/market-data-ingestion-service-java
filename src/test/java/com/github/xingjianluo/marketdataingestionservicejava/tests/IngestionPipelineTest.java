package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregation.AggregationService;
import com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties;
import com.github.xingjianluo.marketdataingestionservicejava.ingest.EventNormalizer;
import com.github.xingjianluo.marketdataingestionservicejava.ingest.MarketDataIngestor;
import com.github.xingjianluo.marketdataingestionservicejava.instrument.InstrumentRegistry;
import com.github.xingjianluo.marketdataingestionservicejava.journal.FileEventJournal;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketFeed;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端接入管线：模拟源场景 -> ingestor -> 聚合/日志，
 * 重点断言“结论一致且失败原因可区分”。
 */
class IngestionPipelineTest {

    private static final Logger log = LoggerFactory.getLogger(IngestionPipelineTest.class);

    @TempDir
    Path tempDir;

    private Harness newHarness() {
        var props = new MarketDataProperties();
        props.setJournalPath(tempDir.resolve("journal-" + System.nanoTime() + ".jsonl").toString());
        props.setMaxOutOfOrderMillis(5_000L);
        try {
            var journal = new FileEventJournal(props);
            journal.init();
            var registry = new InstrumentRegistry(props);
            var normalizer = new EventNormalizer(registry);
            var agg = new AggregationService(props);
            var ingestor = new MarketDataIngestor(normalizer, agg, journal);
            ingestor.recover();
            return new Harness(ingestor, agg, journal);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    record Harness(MarketDataIngestor ingestor, AggregationService agg, FileEventJournal journal) {
    }

    private List<IngestionOutcome> runScenario(Harness h, String name) {
        LocalMarketFeed feed = new LocalMarketFeed();
        List<IngestionOutcome> outcomes = new ArrayList<>();
        log.info("===== 场景: {} =====", name);
        for (var raw : feed.scenario(name)) {
            IngestionOutcome o = h.ingestor.ingest(raw);
            log.info("输入={} => 结论={}, 原因={}, 依据={}",
                    raw, o.status(), o.reason(), o.detail());
            outcomes.add(o);
        }
        return outcomes;
    }

    private long count(List<IngestionOutcome> os, IngestionStatus st) {
        return os.stream().filter(o -> o.status() == st).count();
    }

    private boolean hasReason(List<IngestionOutcome> os, RejectReason r) {
        return os.stream().anyMatch(o -> o.reason() == r);
    }

    @Test
    void happyAndOutOfOrderProduceSameFinalAggregate() {
        Harness h1 = newHarness();
        Harness h2 = newHarness();

        var happy = runScenario(h1, "happy");
        var ooo = runScenario(h2, "out-of-order");

        happy.forEach(o -> assertEquals(IngestionStatus.ACCEPTED, o.status(), o.detail()));
        ooo.forEach(o -> assertEquals(IngestionStatus.ACCEPTED, o.status(),
                "窗口内乱序（缓冲+排空）最终都应接受: " + o.detail()));

        var s1 = h1.agg.snapshot("BTC-USD");
        var s2 = h2.agg.snapshot("ETH-USD");
        log.info("happy 快照={}", s1);
        log.info("out-of-order 快照={}", s2);
        // 仅比较业务聚合字段（事件时间随取数时刻变化，不参与一致性断言）
        assertEquals(s1.lastSequence(), s2.lastSequence());
        assertEquals(s1.lastTradePrice(), s2.lastTradePrice());
        assertEquals(s1.totalVolume(), s2.totalVolume());
        assertEquals(s1.tradeCount(), s2.tradeCount());
        assertEquals(s1.highPrice(), s2.highPrice());
        assertEquals(s1.lowPrice(), s2.lowPrice());
        assertEquals(s1.vwap(), s2.vwap(), 1e-9);
        assertEquals(s1.bestBidPrice(), s2.bestBidPrice());
        assertEquals(s1.bestAskPrice(), s2.bestAskPrice());
        assertEquals(s1.acceptedCount(), s2.acceptedCount());
        assertEquals(s1.rejectedCount(), s2.rejectedCount());
        assertEquals(s1.duplicateCount(), s2.duplicateCount());
    }

    @Test
    void duplicateIsReportedAsDuplicateNotRejected() {
        var os = runScenario(newHarness(), "duplicate");
        assertEquals(2, count(os, IngestionStatus.ACCEPTED));
        assertEquals(1, count(os, IngestionStatus.DUPLICATE));
        assertEquals(0, count(os, IngestionStatus.REJECTED));
        assertTrue(hasReason(os, RejectReason.DUPLICATE_OFFSET));
    }

    @Test
    void contradictionGapRollbackLateCrossedAreDistinctRejections() {
        var contradiction = runScenario(newHarness(), "contradiction");
        assertTrue(hasReason(contradiction, RejectReason.SEQUENCE_CONFLICT),
                "标的层同序号篡改必须是 SEQUENCE_CONFLICT");
        assertEquals(1, count(contradiction, IngestionStatus.REJECTED));

        var offsetTamper = runScenario(newHarness(), "offset-tamper");
        assertTrue(hasReason(offsetTamper, RejectReason.OFFSET_ROLLBACK),
                "源位点层同位点篡改必须是 OFFSET_ROLLBACK，与序号矛盾区分");
        assertEquals(1, count(offsetTamper, IngestionStatus.REJECTED));

        var gap = runScenario(newHarness(), "gap");
        assertTrue(hasReason(gap, RejectReason.SEQUENCE_GAP));

        var rollback = runScenario(newHarness(), "rollback");
        assertTrue(hasReason(rollback, RejectReason.OFFSET_ROLLBACK));

        var late = runScenario(newHarness(), "late");
        assertTrue(hasReason(late, RejectReason.LATE_OUTSIDE_WINDOW),
                "窗口外迟到必须明确标记，不得混入结果");

        var crossed = runScenario(newHarness(), "crossed-trade");
        assertTrue(hasReason(crossed, RejectReason.TRADE_PRICE_CONTRADICTS_QUOTE),
                "穿价成交必须与其它原因区分");

        // 超宽缺口被明确拒绝，聚合停留在 seq2
        var gapHarness = newHarnessAfterRun("gap");
        var gapSnap = gapHarness.agg.snapshot("DOGE-USD");
        assertEquals(2L, gapSnap.acceptedCount(), "缺口事件不得计入");
        assertEquals(2L, gapSnap.lastSequence(), "缺口导致序号停留在 2");
        assertEquals(1L, gapSnap.rejectedCount());

        var rollbackSnap = newHarnessAfterRun("rollback").agg.snapshot("DOT-USD");
        assertEquals(2L, rollbackSnap.acceptedCount());
        assertEquals(1L, rollbackSnap.rejectedCount());
    }

    @Test
    void invalidScenarioCoversAllNormalizationReasons() {
        var os = runScenario(newHarness(), "invalid");
        for (RejectReason r : List.of(
                RejectReason.MISSING_SOURCE_ID,
                RejectReason.INVALID_TIMESTAMP,
                RejectReason.UNKNOWN_SYMBOL,
                RejectReason.INCONSISTENT_QUOTE,
                RejectReason.UNSUPPORTED_TYPE)) {
            assertTrue(hasReason(os, r), "非法场景应包含原因: " + r);
        }
        assertEquals(5, count(os, IngestionStatus.REJECTED));
        assertEquals(0, count(os, IngestionStatus.ACCEPTED));
    }

    @Test
    void onlyAcceptedEventsAreJournaledAndReplayRebuildsState() {
        Harness h = newHarness();
        var os = runScenario(h, "mixed");
        long accepted = count(os, IngestionStatus.ACCEPTED);
        long duplicate = count(os, IngestionStatus.DUPLICATE);
        long rejected = count(os, IngestionStatus.REJECTED);
        log.info("mixed 统计 accepted={}, duplicate={}, rejected={}", accepted, duplicate, rejected);
        assertTrue(accepted >= 3);
        assertTrue(duplicate >= 1 || rejected >= 1);

        // 日志中只能有“已接受”事件
        var journaled = h.journal.replayAll();
        assertEquals(accepted, journaled.size(), "只有 ACCEPTED 才允许进入日志");

        // 用同一日志恢复到全新服务，状态一致
        var beforeBtc = h.agg.snapshot("BTC-USD");
        var beforeEth = h.agg.snapshot("ETH-USD");
        var props = new MarketDataProperties();
        props.setJournalPath(h.journal.path().toString());
        try {
            var journal2 = new FileEventJournal(props);
            journal2.init();
            var agg2 = new AggregationService(props);
            var ing2 = new MarketDataIngestor(
                    new EventNormalizer(new InstrumentRegistry(props)), agg2, journal2);
            ing2.recover();
            // 恢复只重建“已接受”的聚合状态；历史重复/拒绝计数不持久化、重置为 0
            var afterBtc = agg2.snapshot("BTC-USD");
            var afterEth = agg2.snapshot("ETH-USD");
            assertEquals(beforeBtc.acceptedCount(), afterBtc.acceptedCount());
            assertEquals(beforeBtc.totalVolume(), afterBtc.totalVolume());
            assertEquals(beforeBtc.lastTradePrice(), afterBtc.lastTradePrice());
            assertEquals(beforeBtc.vwap(), afterBtc.vwap(), 1e-9);
            assertEquals(beforeEth.acceptedCount(), afterEth.acceptedCount());
            assertEquals(beforeEth.totalVolume(), afterEth.totalVolume());
            assertEquals(0L, afterBtc.duplicateCount(), "历史重复不重放、不重建");
            assertEquals(0L, afterBtc.rejectedCount());
            assertEquals(0L, afterEth.duplicateCount());
            assertEquals(0L, afterEth.rejectedCount());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** 语义化辅助：运行某场景后返回 harness。 */
    private Harness newHarnessAfterRun(String scenario) {
        Harness h = newHarness();
        runScenario(h, scenario);
        return h;
    }
}
