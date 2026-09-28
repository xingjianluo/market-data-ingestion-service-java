package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregate.MarketDataAggregator;
import com.github.xingjianluo.marketdataingestionservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.InstrumentRegistry;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.MarketDataNormalizer;
import com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore;
import com.github.xingjianluo.marketdataingestionservicejava.replay.SourceSequenceTracker;
import com.github.xingjianluo.marketdataingestionservicejava.service.IngestionService;
import com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 消费本地确定性脚本，锁定乱序/重复/矛盾场景的逐帧判定与最终聚合，
 * 防止脚本与判定规则在后续修改中悄悄漂移。
 */
class ScenarioScriptTests {

    private static final Logger log = LoggerFactory.getLogger(ScenarioScriptTests.class);

    private IngestionService newService(Path stateFile, long windowMillis) {
        var props = new ServiceProperties();
        props.setStateFile(stateFile.toString());
        props.setLateWindowMillis(windowMillis);
        var clock = java.time.Clock.systemUTC();
        var store = new ReplayStateStore(new ObjectMapper(), props);
        var service = new IngestionService(
                new MarketDataNormalizer(new InstrumentRegistry(), clock),
                new SourceSequenceTracker(),
                new MarketDataAggregator(clock, props), store);
        service.restore();
        return service;
    }

    @Test
    @DisplayName("out-of-order-dup 脚本：判定序列与聚合数值确定")
    void outOfOrderDupScenarioDeterministic() throws Exception {
        Path state = Files.createTempFile("scn-", ".json");
        Files.delete(state);
        var service = newService(state, 100_000);
        var batch = new LocalMarketDataSource().batch("out-of-order-dup");

        List<IngestionStatus> statuses = new ArrayList<>();
        List<RejectReason> reasons = new ArrayList<>();
        for (var raw : batch.records()) {
            var o = service.ingest(raw);
            statuses.add(o.status());
            reasons.add(o.rejectReason());
            log.info("帧 seq={} => {} reason={} | {}", raw.sequence(), o.status(),
                    o.rejectReason() != null ? o.rejectReason() : o.duplicateReason(),
                    o.message());
        }

        // 期望序列：
        // seq1 接受；seq3 缺口；seq2 接受；seq3 重传接受；seq4 接受；
        // seq1 完全重复；seq2 矛盾拒绝；seq5 接受
        List<IngestionStatus> expectedStatus = List.of(
                IngestionStatus.ACCEPTED,   // seq=1
                IngestionStatus.REJECTED,   // seq=3 GAP
                IngestionStatus.ACCEPTED,   // seq=2
                IngestionStatus.ACCEPTED,   // seq=3 重传
                IngestionStatus.ACCEPTED,   // seq=4
                IngestionStatus.DUPLICATE,  // seq=1 重复
                IngestionStatus.REJECTED,   // seq=2 矛盾
                IngestionStatus.ACCEPTED    // seq=5
        );
        assertEquals(expectedStatus, statuses);
        assertEquals(RejectReason.SEQUENCE_GAP, reasons.get(1));
        assertEquals(RejectReason.CONTRADICTORY_RECORD, reasons.get(6));

        var v = service.aggregateOf("600519.SH").orElseThrow();
        log.info("聚合 accepted={} dup={} rejected={} volume={} vwap={} last={} bid={} ask={}",
                v.acceptedCount(), v.duplicateCount(), v.rejectedCount(), v.totalVolume(),
                v.vwap(), v.lastTradePrice(), v.latestBid(), v.latestAsk());
        assertEquals(5, v.acceptedCount());      // seq 1,2,3,4,5
        assertEquals(1, v.duplicateCount());     // seq1 重发
        assertEquals(2, v.rejectedCount());      // 缺口 + 矛盾
        assertEquals(0, v.totalVolume().compareTo(new BigDecimal("35"))); // 10+20+5
        assertEquals(0, v.lastTradePrice().compareTo(new BigDecimal("101.00")));
    }
}
