package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregation.AggregationService;
import com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.github.xingjianluo.marketdataingestionservicejava.tests.TestEvents.trade;
import static com.github.xingjianluo.marketdataingestionservicejava.tests.TestEvents.tradeFrom;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 聚合服务：源位点层的回退/重复/篡改/跳空，以及跨标的独立性。
 */
class AggregationServiceTest {

    private static final Logger log = LoggerFactory.getLogger(AggregationServiceTest.class);

    private AggregationService newService() {
        return new AggregationService(new MarketDataProperties());
    }

    private AggregationService.Result run(AggregationService svc, NormalizedEvent e) {
        var r = svc.process(e);
        log.info("输入(source={}, offset={}, symbol={}, seq={}) => 判定={}, 原因={}, 依据={}",
                e.sourceId(), e.offset(), e.symbol(), e.sequence(),
                r.outcome().status(), r.outcome().reason(), r.outcome().detail());
        return r;
    }

    @Test
    void sourceOffsetRollbackAndDuplicateAreDistinguished() {
        long t = 5_000_000L;
        AggregationService svc = newService();

        var a1 = run(svc, tradeFrom("sA", 30L, "BTC-USD", 1, t, 100.0, 1.0));
        var a2 = run(svc, tradeFrom("sA", 31L, "BTC-USD", 2, t + 10, 100.1, 1.0));
        assertEquals(IngestionStatus.ACCEPTED, a1.outcome().status());
        assertEquals(IngestionStatus.ACCEPTED, a2.outcome().status());

        // 明显回退：从未见过、且远落后于下界（max=31, 下界=15）的位点
        var rollback = run(svc, tradeFrom("sA", 1L, "BTC-USD", 3, t + 20, 100.2, 1.0));
        assertEquals(IngestionStatus.REJECTED, rollback.outcome().status());
        assertEquals(RejectReason.OFFSET_ROLLBACK, rollback.outcome().reason());

        // 同位点重复投递：offset 31 内容一致 => 已重复（幂等）
        var dup = run(svc, tradeFrom("sA", 31L, "BTC-USD", 2, t + 10, 100.1, 1.0));
        assertEquals(IngestionStatus.DUPLICATE, dup.outcome().status());
        assertEquals(RejectReason.DUPLICATE_OFFSET, dup.outcome().reason());

        // 同位点篡改：offset 31 内容不同 => OFFSET_ROLLBACK（拒绝，区别于幂等重复）
        var tampered = run(svc, tradeFrom("sA", 31L, "BTC-USD", 2, t + 10, 888.0, 1.0));
        assertEquals(IngestionStatus.REJECTED, tampered.outcome().status());
        assertEquals(RejectReason.OFFSET_ROLLBACK, tampered.outcome().reason());

        // 位点跳空：offset 远超高水位 + 缓冲宽度
        var gap = run(svc, tradeFrom("sA", 60L, "BTC-USD", 3, t + 30, 100.2, 1.0));
        assertEquals(RejectReason.SEQUENCE_GAP, gap.outcome().reason());

        var snap = svc.snapshot("BTC-USD");
        assertEquals(2L, snap.acceptedCount(), "回退/重复/篡改/跳空都不得累加");
        assertEquals(100.1, snap.lastTradePrice());
        assertEquals(1L, snap.duplicateCount());
        assertEquals(3L, snap.rejectedCount());
        log.info("源位点异常后快照={}", snap);
    }

    @Test
    void offsetsAreTrackedPerSourceAndAggregatesPerSymbol() {
        long t = 6_000_000L;
        AggregationService svc = newService();

        // 两个源各自从 offset=1 开始，互不影响
        assertEquals(IngestionStatus.ACCEPTED,
                run(svc, tradeFrom("sA", 1L, "BTC-USD", 1, t, 100.0, 1.0))
                        .outcome().status());
        assertEquals(IngestionStatus.ACCEPTED,
                run(svc, tradeFrom("sB", 1L, "ETH-USD", 1, t, 3000.0, 1.0))
                        .outcome().status());
        // sA 的 offset=2 与 sB 的 offset=2 独立
        assertEquals(IngestionStatus.ACCEPTED,
                run(svc, tradeFrom("sA", 2L, "BTC-USD", 2, t + 10, 100.2, 2.0))
                        .outcome().status());
        assertEquals(IngestionStatus.ACCEPTED,
                run(svc, tradeFrom("sB", 2L, "ETH-USD", 2, t + 10, 3000.2, 1.0))
                        .outcome().status());

        // sB 重发一个明显回退、从未见过的位点（offset 远落后）不影响 sA
        var rb = run(svc, tradeFrom("sB", 100L, "ETH-USD", 3, t + 20, 3000.3, 1.0));
        assertEquals(RejectReason.SEQUENCE_GAP, rb.outcome().reason());

        var btc = svc.snapshot("BTC-USD");
        var eth = svc.snapshot("ETH-USD");
        log.info("BTC={}, ETH={}", btc, eth);
        assertEquals(2L, btc.acceptedCount());
        assertEquals(2L, eth.acceptedCount());
        assertEquals(0L, btc.rejectedCount());
        assertEquals(1L, eth.rejectedCount());
    }

    @Test
    void replayIsIdempotentAndDoesNotDoubleCount() {
        long t = 7_000_000L;
        List<NormalizedEvent> history = List.of(
                tradeFrom("sA", 30L, "BTC-USD", 1, t, 100.0, 2.0),
                tradeFrom("sA", 31L, "BTC-USD", 2, t + 10, 100.4, 3.0));

        // 模拟“重启两次”：两个全新实例各自加载同一批日志，状态必须一致
        AggregationService svc1 = newService();
        AggregationService svc2 = newService();
        history.forEach(svc1::replay);
        history.forEach(svc2::replay);

        var snap1 = svc1.snapshot("BTC-USD");
        var snap2 = svc2.snapshot("BTC-USD");
        log.info("重放后实例1={}", snap1);
        log.info("重放后实例2={}", snap2);
        assertEquals(snap1, snap2, "同批日志重建结果必须一致");
        assertEquals(2L, snap1.acceptedCount());
        assertEquals(5.0, snap1.totalVolume(), 1e-9, "恢复重建必须精确累加成交量一次");
        assertEquals(0L, snap1.duplicateCount());
        assertEquals(0L, snap1.rejectedCount());

        // 重启后再次收到“最近一条”的相同报文（同位点、同内容）=> 已重复，幂等
        var redeliverLast = svc1.process(history.get(1));
        assertEquals(IngestionStatus.DUPLICATE, redeliverLast.outcome().status());
        assertEquals(RejectReason.DUPLICATE_OFFSET, redeliverLast.outcome().reason());

        // 重启后收到一条“更早位点”的原始报文：该位点已见过、内容一致 => 同样按重复幂等
        var redeliverOldSeen = svc1.process(history.get(0));
        assertEquals(IngestionStatus.DUPLICATE, redeliverOldSeen.outcome().status());
        assertEquals(RejectReason.DUPLICATE_OFFSET, redeliverOldSeen.outcome().reason());

        // 真正的位点回退：max=31（下界 15），发一个从未见过且远落后的位点 => OFFSET_ROLLBACK
        var farBehind = svc1.process(
                tradeFrom("sA", 1L, "BTC-USD", 3, t + 30, 100.5, 1.0));
        assertEquals(RejectReason.OFFSET_ROLLBACK, farBehind.outcome().reason());

        var snap1b = svc1.snapshot("BTC-USD");
        log.info("重复/回退再投递后实例1={}", snap1b);
        assertEquals(2L, snap1b.acceptedCount());
        assertEquals(5.0, snap1b.totalVolume(), 1e-9);
        assertEquals(2L, snap1b.duplicateCount());
        assertEquals(1L, snap1b.rejectedCount());
    }

    @Test
    void gapHoleIsVisibleAndFillingItDrainsBuffer() {
        long t = 8_000_000L;
        AggregationService svc = newService();
        List<String> statuses = new ArrayList<>();

        // offset 连续、但 symbol 序号乱序：1,3,2
        var r1 = run(svc, tradeFrom("sA", 1L, "BTC-USD", 1, t, 100.0, 1.0));
        var r3 = run(svc, tradeFrom("sA", 2L, "BTC-USD", 3, t + 20, 100.3, 1.0)); // held
        var r2 = run(svc, tradeFrom("sA", 3L, "BTC-USD", 2, t + 10, 100.2, 1.0)); // drains 3
        for (var r : List.of(r1, r3, r2)) {
            statuses.add(r.outcome().status().name());
        }
        var snap = svc.snapshot("BTC-USD");
        log.info("乱序补齐后状态序列={}, 快照={}", statuses, snap);
        assertEquals(3L, snap.acceptedCount());
        assertEquals(3L, snap.lastSequence());
        // seq1 立即应用；seq3 缓冲（applied 为空）；seq2 到达后排空 [seq2, seq3]
        assertEquals(List.of(1L),
                r1.applied().stream().map(NormalizedEvent::sequence).toList());
        assertEquals(List.of(),
                r3.applied().stream().map(NormalizedEvent::sequence).toList());
        assertEquals(List.of(2L, 3L),
                r2.applied().stream().map(NormalizedEvent::sequence).toList());
    }
}
