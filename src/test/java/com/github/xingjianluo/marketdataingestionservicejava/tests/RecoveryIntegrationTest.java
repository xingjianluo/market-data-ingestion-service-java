package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestionservicejava.aggregation.AggregationService;
import com.github.xingjianluo.marketdataingestionservicejava.ingest.MarketDataIngestor;
import com.github.xingjianluo.marketdataingestionservicejava.model.AggregateSnapshot;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 端到端重启恢复：同一日志文件支撑的两次上下文启动，聚合状态必须一致；
 * 重启后重投同一批数据，必须识别为已重复/位点回退，不重复累加。
 *
 * <p>使用 target 下的独立日志文件，避免与默认路径或其它测试相互干扰。
 */
@SpringBootTest
@TestPropertySource(properties = {
        "market-data.journal-path=target/recovery-it/journal.jsonl",
        "market-data.max-out-of-order-millis=5000"
})
class RecoveryIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryIntegrationTest.class);
    private static final java.nio.file.Path JOURNAL =
            java.nio.file.Path.of("target/recovery-it/journal.jsonl");

    // 在 Spring 上下文创建（触发启动恢复）之前清空日志，保证测试隔离
    static {
        try {
            java.nio.file.Files.deleteIfExists(JOURNAL);
        } catch (java.io.IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Autowired
    private MarketDataIngestor ingestor;
    @Autowired
    private AggregationService aggregationService;

    @Test
    void sameBatchAfterRestartIsConsistentAndIdempotent() {
        long t = System.currentTimeMillis() - 1000L;
        // 使用较高位点（30..33），便于在恢复后构造“未见过且低于回退下界”的脏重放
        List<RawMarketEvent> batch = List.of(
                quote(30, "BTC-USD", 1, t, 100.0, 1.0, 100.5, 1.0),
                trade(31, "BTC-USD", 2, t + 10, 100.2, 2.0),
                quote(32, "ETH-USD", 1, t, 3000.0, 1.0, 3000.5, 1.0),
                trade(33, "ETH-USD", 2, t + 10, 3000.2, 3.0)
        );

        log.info("=== 首次投递一批 {} 条 ===", batch.size());
        for (RawMarketEvent raw : batch) {
            IngestionOutcome o = ingestor.ingest(raw);
            log.info("输入={} => 结论={}, 原因={}, 依据={}",
                    raw, o.status(), o.reason(), o.detail());
            assertEquals(IngestionStatus.ACCEPTED, o.status(),
                    "首次投递应全部接受: " + o.detail());
        }

        AggregateSnapshot btcBefore = aggregationService.snapshot("BTC-USD");
        AggregateSnapshot ethBefore = aggregationService.snapshot("ETH-USD");
        log.info("重启前 BTC={}", btcBefore);
        log.info("重启前 ETH={}", ethBefore);

        // 重启：新建一套服务组件（同一日志文件），模拟进程重启后的恢复
        RecoveryHarness restarted = RecoveryHarness.boot(JOURNAL.toString());
        AggregateSnapshot btcAfter = restarted.snapshot("BTC-USD");
        AggregateSnapshot ethAfter = restarted.snapshot("ETH-USD");
        log.info("重启后 BTC={}", btcAfter);
        log.info("重启后 ETH={}", ethAfter);

        assertEquals(btcBefore, btcAfter, "重启恢复后 BTC 状态必须与重启前一致");
        assertEquals(ethBefore, ethAfter, "重启恢复后 ETH 状态必须与重启前一致");
        assertEquals(4L, restarted.replayedCount(), "应从日志重放 4 条已接受事件");

        // 重启后重投最后一条相同报文 => DUPLICATE_OFFSET（已重复）
        IngestionOutcome redeliver = restarted.ingest(batch.get(3));
        log.info("重启后重投相同报文 => {}", redeliver);
        assertEquals(IngestionStatus.DUPLICATE, redeliver.status());
        assertEquals(RejectReason.DUPLICATE_OFFSET, redeliver.reason());

        // 重启后重投最早一条相同报文：位点已见过、内容一致 => DUPLICATE_OFFSET（已重复）
        IngestionOutcome redeliverOld = restarted.ingest(batch.get(0));
        log.info("重启后重投已见过的旧位点相同报文 => {}", redeliverOld);
        assertEquals(IngestionStatus.DUPLICATE, redeliverOld.status());
        assertEquals(RejectReason.DUPLICATE_OFFSET, redeliverOld.reason());

        // 位点回退重放：一条从未见过、且远落后于回退下界（max=33, 下界=17）的报文
        RawMarketEvent rolledBack = new RawMarketEvent(
                "local-feed-1", 10L, "BTC-USD", 3L, "TRADE", t + 600L,
                1.0, 1.0, null, null, null, null);
        IngestionOutcome rollback = restarted.ingest(rolledBack);
        log.info("重启后收到位点回退重放报文 => {}", rollback);
        assertEquals(IngestionStatus.REJECTED, rollback.status());
        assertEquals(RejectReason.OFFSET_ROLLBACK, rollback.reason());

        // 业务聚合未发生任何累加；仅计数体现“2 次重复、1 次拒绝”
        AggregateSnapshot ethFinal = restarted.snapshot("ETH-USD");
        AggregateSnapshot btcFinal = restarted.snapshot("BTC-USD");
        assertEquals(ethAfter.acceptedCount(), ethFinal.acceptedCount());
        assertEquals(ethAfter.totalVolume(), ethFinal.totalVolume());
        assertEquals(ethAfter.lastTradePrice(), ethFinal.lastTradePrice());
        assertEquals(btcAfter.acceptedCount(), btcFinal.acceptedCount());
        assertEquals(btcAfter.totalVolume(), btcFinal.totalVolume());
        assertEquals(1L, ethFinal.duplicateCount(), "ETH 重投应被记为 1 次重复");
        assertEquals(1L, btcFinal.duplicateCount(), "BTC 重投旧报文应被记为 1 次重复");
        assertEquals(1L, btcFinal.rejectedCount(), "回退重放应被记为 1 次拒绝");
        assertNotNull(btcFinal.vwap());
    }

    private static RawMarketEvent quote(long offset, String symbol, long seq, long t,
                                        double bid, double bidSize,
                                        double ask, double askSize) {
        return new RawMarketEvent("local-feed-1", offset, symbol, seq, "QUOTE", t,
                null, null, bid, bidSize, ask, askSize);
    }

    private static RawMarketEvent trade(long offset, String symbol, long seq, long t,
                                        double price, double size) {
        return new RawMarketEvent("local-feed-1", offset, symbol, seq, "TRADE", t,
                price, size, null, null, null, null);
    }

    /** 不启动第二个 Spring 上下文，直接用生产组件手工装配一套，指向同一日志文件。 */
    static final class RecoveryHarness {
        private final MarketDataIngestor ingestor;
        private final AggregationService aggregationService;
        private final long replayed;

        private RecoveryHarness(MarketDataIngestor ingestor,
                                AggregationService aggregationService, long replayed) {
            this.ingestor = ingestor;
            this.aggregationService = aggregationService;
            this.replayed = replayed;
        }

        static RecoveryHarness boot(String journalPath) {
            var props = new com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties();
            props.setJournalPath(journalPath);
            props.setMaxOutOfOrderMillis(5_000L);
            var registry = new com.github.xingjianluo.marketdataingestionservicejava.instrument.InstrumentRegistry(props);
            var normalizer = new com.github.xingjianluo.marketdataingestionservicejava.ingest.EventNormalizer(registry);
            var agg = new AggregationService(props);
            var journal = new com.github.xingjianluo.marketdataingestionservicejava.journal.FileEventJournal(props);
            try {
                journal.init();
            } catch (java.io.IOException e) {
                throw new RuntimeException(e);
            }
            var ing = new MarketDataIngestor(normalizer, agg, journal);
            ing.recover();
            return new RecoveryHarness(ing, agg, ing.replayedCount());
        }

        IngestionOutcome ingest(RawMarketEvent raw) {
            return ingestor.ingest(raw);
        }

        AggregateSnapshot snapshot(String symbol) {
            return aggregationService.snapshot(symbol);
        }

        long replayedCount() {
            return replayed;
        }
    }
}
