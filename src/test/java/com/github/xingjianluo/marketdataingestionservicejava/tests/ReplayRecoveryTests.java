package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregate.MarketDataAggregator;
import com.github.xingjianluo.marketdataingestionservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.DuplicateReason;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.ReplayVerdict;
import com.github.xingjianluo.marketdataingestionservicejava.model.SymbolAggregateView;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.InstrumentRegistry;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.MarketDataNormalizer;
import com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayCoordinator;
import com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore;
import com.github.xingjianluo.marketdataingestionservicejava.replay.SourceSequenceTracker;
import com.github.xingjianluo.marketdataingestionservicejava.service.IngestionService;
import com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重放恢复：位点提交、快照落盘、重启恢复、位点回退与序号缺口在重放中的表现。
 */
class ReplayRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(ReplayRecoveryTests.class);

    private Path stateFile;
    private ServiceProperties properties;
    private LocalMarketDataSource dataSource;

    /** 每个字段都是全新对象，但共享同一个快照文件 —— 模拟“重启”。 */
    private static final class Restarted {
        IngestionService service;
        ReplayCoordinator coordinator;
        SourceSequenceTracker tracker;
        MarketDataAggregator aggregator;
    }

    @BeforeEach
    void setUp() throws Exception {
        stateFile = Files.createTempFile("mdis-replay-", ".json");
        Files.delete(stateFile);
        properties = new ServiceProperties();
        properties.setStateFile(stateFile.toString());
        properties.setLateWindowMillis(100_000);
        dataSource = new LocalMarketDataSource();
    }

    private Restarted restart() {
        var clock = java.time.Clock.systemUTC();
        var normalizer = new MarketDataNormalizer(new InstrumentRegistry(), clock);
        var tracker = new SourceSequenceTracker();
        var aggregator = new MarketDataAggregator(clock, properties);
        var store = new ReplayStateStore(new ObjectMapper(), properties);
        var service = new IngestionService(normalizer, tracker, aggregator, store);
        service.restore();
        var coordinator = new ReplayCoordinator(service, dataSource, tracker);
        Restarted r = new Restarted();
        r.service = service;
        r.coordinator = coordinator;
        r.tracker = tracker;
        r.aggregator = aggregator;
        return r;
    }

    private void logOutcomes(String label, ReplayCoordinator.ReplaySession session) {
        log.info("[{}] verdict={} fromOffset={} committed={} | {}",
                label, session.start().verdict(), session.start().fromOffset(),
                session.start().committedOffset(), session.start().message());
        for (IngestionOutcome o : session.outcomes()) {
            log.info("[{}] => {} reason={} | {}", label, o.status(),
                    o.rejectReason() != null ? o.rejectReason() : o.duplicateReason(),
                    o.message());
        }
    }

    @Test
    @DisplayName("重启后快照恢复：聚合数值与位点完全保持")
    void snapshotSurvivesRestart() {
        Restarted first = restart();
        var batch = dataSource.batch("ordered");
        // 用带位点的接入方式提交全部 5 帧
        for (int i = 0; i < batch.size(); i++) {
            var o = first.service.ingestAt(batch.records().get(i), i);
            assertEquals(IngestionStatus.ACCEPTED, o.status(),
                    "首遍应全部接受: " + o.message());
        }
        SymbolAggregateView before = first.service.aggregateOf("600519.SH").orElseThrow();
        long committedBefore = first.tracker.committedOffset("SIM-A");
        log.info("重启前 volume={} vwap={} accepted={} committed={}",
                before.totalVolume(), before.vwap(), before.acceptedCount(), committedBefore);
        assertEquals(4L, committedBefore);

        // “重启”：全新组件实例，从同一文件恢复
        Restarted second = restart();
        SymbolAggregateView after = second.service.aggregateOf("600519.SH").orElseThrow();
        long committedAfter = second.tracker.committedOffset("SIM-A");
        log.info("重启后 volume={} vwap={} accepted={} committed={}",
                after.totalVolume(), after.vwap(), after.acceptedCount(), committedAfter);

        assertEquals(before.acceptedCount(), after.acceptedCount());
        assertEquals(0, before.totalVolume().compareTo(after.totalVolume()));
        assertEquals(before.vwap(), after.vwap());
        assertEquals(before.lastTradePrice(), after.lastTradePrice());
        assertEquals(committedBefore, committedAfter);
    }

    @Test
    @DisplayName("重启后重放同一批：全部判重，聚合不重复累加")
    void replayAfterRestartIsIdempotent() {
        Restarted first = restart();
        var session1 = first.coordinator.replay("ordered", 0);
        logOutcomes("首遍重放", session1);
        assertEquals(ReplayVerdict.OK, session1.start().verdict());
        BigDecimal volumeAfterFirst =
                first.service.aggregateOf("600519.SH").orElseThrow().totalVolume();
        long acceptedAfterFirst =
                first.service.aggregateOf("600519.SH").orElseThrow().acceptedCount();

        Restarted second = restart();
        var session2 = second.coordinator.replay("ordered", 0);
        logOutcomes("重启后从头重放", session2);
        // 位点回退可被明确识别
        assertEquals(ReplayVerdict.OFFSET_ROLLBACK, session2.start().verdict());
        long duplicates = session2.outcomes().stream()
                .filter(o -> o.status() == IngestionStatus.DUPLICATE
                        && o.duplicateReason() == DuplicateReason.ALREADY_COMMITTED_OFFSET)
                .count();
        log.info("重启后重放判重帧数={}", duplicates);
        assertEquals(session2.outcomes().size(), duplicates);

        SymbolAggregateView v = second.service.aggregateOf("600519.SH").orElseThrow();
        assertEquals(0, v.totalVolume().compareTo(volumeAfterFirst), "成交量不得翻倍");
        assertEquals(acceptedAfterFirst, v.acceptedCount(), "接受数不得增加");
    }

    @Test
    @DisplayName("位点回退重放：回退帧判 ALREADY_COMMITTED_OFFSET，新帧正常接受")
    void partialRollbackIsExplainable() {
        Restarted r = restart();
        r.coordinator.replay("ordered", 0);
        long committed = r.tracker.committedOffset("SIM-A");
        log.info("已提交位点={}", committed);

        // 从 offset=3 回退重放：帧 3、4 已提交判重；本脚本没有更多新帧
        var rollback = r.coordinator.replay("ordered", 3);
        logOutcomes("回退到3", rollback);
        assertEquals(ReplayVerdict.OFFSET_ROLLBACK, rollback.start().verdict());
        assertTrue(rollback.outcomes().stream()
                .allMatch(o -> o.status() == IngestionStatus.DUPLICATE
                        && o.duplicateReason() == DuplicateReason.ALREADY_COMMITTED_OFFSET));
    }

    @Test
    @DisplayName("重放跳位点：缺口帧给出 SEQUENCE_GAP，与位点回退原因可区分")
    void replayWithGap() {
        Restarted r = restart();
        // ordered 脚本 5 帧，从 offset=2 开始（跳过 seq=1,2）：首帧 seq=3 相对全新水位是缺口
        var session = r.coordinator.replay("ordered", 2);
        logOutcomes("跳位点重放", session);
        assertEquals(ReplayVerdict.OK, session.start().verdict(),
                "向未来位点移动不是回退");
        long gaps = session.outcomes().stream()
                .filter(o -> o.rejectReason() == com.github.xingjianluo.marketdataingestionservicejava
                        .model.RejectReason.SEQUENCE_GAP)
                .count();
        log.info("跳位点重放中的 SEQUENCE_GAP 帧数={}", gaps);
        assertTrue(gaps >= 1, "跳过序号必须报 SEQUENCE_GAP");
    }
}
