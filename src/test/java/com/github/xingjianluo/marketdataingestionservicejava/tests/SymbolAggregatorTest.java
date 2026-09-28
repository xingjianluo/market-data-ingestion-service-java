package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregation.SymbolAggregator;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.github.xingjianluo.marketdataingestionservicejava.tests.TestEvents.quote;
import static com.github.xingjianluo.marketdataingestionservicejava.tests.TestEvents.trade;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 单标的聚合器：乱序缓冲/排空、重复、矛盾、缺口、迟到、穿价与计数。
 */
class SymbolAggregatorTest {

    private static final Logger log = LoggerFactory.getLogger(SymbolAggregatorTest.class);
    private static final String S = "BTC-USD";

    /** 评估并在需要时由调用方根据 applied 列表应用（此处直接在 evaluate 内部已应用）。 */
    private SymbolAggregator.Decision evalAndLog(SymbolAggregator agg, NormalizedEvent e) {
        var d = agg.evaluate(e);
        log.info("输入(symbol={}, seq={}, offset={}, time={}, type={}) => 判定={}, 原因={}, 本次应用={}, 依据={}",
                e.symbol(), e.sequence(), e.offset(), e.eventTime(), e.type(),
                d.status(), d.reason(), d.applied().size(), d.detail());
        return d;
    }

    /** 顺序喂入一组事件，全部要求最终被接受。 */
    private void feedAndAssertAccepted(SymbolAggregator agg, List<NormalizedEvent> events) {
        for (NormalizedEvent e : events) {
            var d = evalAndLog(agg, e);
            assertEquals(IngestionStatus.ACCEPTED, d.status(),
                    "应被接受（立即或缓冲后）: " + d.detail());
        }
    }

    @Test
    void outOfOrderWithinWindowIsHeldThenDrainedAndMatchesInOrderResult() {
        long t = 1_000_000L;
        SymbolAggregator agg1 = new SymbolAggregator(S, 5_000L);
        SymbolAggregator agg2 = new SymbolAggregator(S, 5_000L);

        List<NormalizedEvent> inOrder = List.of(
                quote(1, S, 1, t, 100.0, 1.0, 100.5, 1.0),
                trade(2, S, 2, t + 10, 100.2, 2.0),
                quote(3, S, 3, t + 20, 100.1, 1.5, 100.6, 1.5),
                trade(4, S, 4, t + 30, 100.3, 3.0));
        feedAndAssertAccepted(agg1, inOrder);

        // 乱序到达：seq3 先于 seq2（事件时间仍在窗口内）
        List<NormalizedEvent> outOfOrder = List.of(
                inOrder.get(0), inOrder.get(2), inOrder.get(1), inOrder.get(3));
        var first = evalAndLog(agg2, outOfOrder.get(0));
        assertEquals(1, first.applied().size());

        var held = evalAndLog(agg2, outOfOrder.get(1)); // seq3 超前
        assertEquals(IngestionStatus.ACCEPTED, held.status());
        assertEquals(0, held.applied().size(), "seq3 应被缓冲，尚未计入聚合");
        assertEquals(1L, agg2.snapshot().acceptedCount(), "缓冲事件不得提前计价");

        var gapFilled = evalAndLog(agg2, outOfOrder.get(2)); // seq2 到达
        assertEquals(2, gapFilled.applied().size(), "seq2 应用并排空缓冲的 seq3");

        var last = evalAndLog(agg2, outOfOrder.get(3));
        assertEquals(1, last.applied().size());

        var s1 = agg1.snapshot();
        var s2 = agg2.snapshot();
        log.info("顺序聚合快照={}", s1);
        log.info("乱序聚合快照={}", s2);
        assertEquals(s1, s2, "乱序排空后与顺序到达的聚合结果必须一致");
        assertEquals(4L, s1.acceptedCount());
        assertEquals((100.2 * 2 + 100.3 * 3) / 5.0, s1.vwap(), 1e-9);
    }

    @Test
    void duplicateAndContradictionAreDistinguished() {
        long t = 2_000_000L;
        SymbolAggregator agg = new SymbolAggregator(S, 5_000L);
        evalAndLog(agg, quote(1, S, 1, t, 100.0, 1.0, 100.5, 1.0));
        evalAndLog(agg, trade(2, S, 2, t + 10, 100.2, 2.0));

        var dup = evalAndLog(agg, trade(2, S, 2, t + 10, 100.2, 2.0));
        assertEquals(IngestionStatus.DUPLICATE, dup.status());
        assertEquals(RejectReason.DUPLICATE_SEQUENCE, dup.reason());

        var conflict = evalAndLog(agg, trade(2, S, 2, t + 10, 999.0, 2.0));
        assertEquals(IngestionStatus.REJECTED, conflict.status());
        assertEquals(RejectReason.SEQUENCE_CONFLICT, conflict.reason());

        var snap = agg.snapshot();
        assertEquals(100.2, snap.lastTradePrice());
        assertEquals(2L, snap.acceptedCount());
        assertEquals(1L, snap.duplicateCount());
        assertEquals(1L, snap.rejectedCount());
        log.info("重复/矛盾后快照={}", snap);
    }

    @Test
    void gapLateAndCrossedTradeAreRejectedWithReasons() {
        long t = 3_000_000L;
        SymbolAggregator agg = new SymbolAggregator(S, 5_000L);
        evalAndLog(agg, quote(1, S, 1, t, 100.0, 1.0, 100.5, 1.0));
        evalAndLog(agg, trade(2, S, 2, t + 10, 100.2, 2.0));

        // seq 跳得远超缓冲宽度（宽度 16）：明确缺口
        var gap = evalAndLog(agg, trade(30, S, 30, t + 30, 100.3, 1.0));
        assertEquals(RejectReason.SEQUENCE_GAP, gap.reason());

        // 窗口内适度超前：先缓冲
        var moderate = evalAndLog(agg, trade(4, S, 4, t + 40, 100.3, 1.0));
        assertEquals(0, moderate.applied().size(), "seq4 适度超前应缓冲而非报缺口");

        // seq3 窗口外迟到（相对已观测水位）：拒绝且单独标记
        var late = evalAndLog(agg, trade(3, S, 3, t - 30_000, 100.1, 1.0));
        assertEquals(RejectReason.LATE_OUTSIDE_WINDOW, late.reason());

        // seq3 穿价：拒绝
        var crossed = evalAndLog(agg, trade(3, S, 3, t + 20, 101.0, 1.0));
        assertEquals(RejectReason.TRADE_PRICE_CONTRADICTS_QUOTE, crossed.reason());

        // 合法 seq3 到达：应用并排空缓冲的 seq4
        var valid = evalAndLog(agg, trade(3, S, 3, t + 20, 100.3, 1.0));
        assertEquals(IngestionStatus.ACCEPTED, valid.status());
        assertEquals(2, valid.applied().size());

        var snap = agg.snapshot();
        assertEquals(4L, snap.acceptedCount(), "迟到/穿价/缺口都不得混入结果");
        assertEquals(3L, snap.rejectedCount());
        assertEquals(1L, snap.lateRejectedCount(), "迟到必须被单独标记");
        assertNotNull(snap.watermark());
        log.info("缺口/迟到/穿价后快照={}", snap);
        assertTrue(snap.lastSequence() == 4L);
    }
}
