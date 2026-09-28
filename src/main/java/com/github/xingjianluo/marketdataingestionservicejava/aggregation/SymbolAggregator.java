package com.github.xingjianluo.marketdataingestionservicejava.aggregation;

import com.github.xingjianluo.marketdataingestionservicejava.model.AggregateSnapshot;
import com.github.xingjianluo.marketdataingestionservicejava.model.EventType;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单标的聚合器：所有读写都在同一把 {@link ReentrantLock} 下串行执行。
 *
 * <p>为同时满足“窗口内乱序可接受”与“序号缺口必须可见”，采用
 * <b>乱序缓冲 + 连续排空</b>：
 * <ul>
 *   <li>事件序号等于 {@code lastSeq+1}：立即应用，并顺势把缓冲区中连续的事件排空；</li>
 *   <li>事件序号超前但不超过缓冲宽度 {@code maxSequenceHold}：先 HELD 缓冲，
 *       等缺号补齐后应用，窗口内迟到不会丢失也不会污染当前结果；</li>
 *   <li>超前超过缓冲宽度：SEQUENCE_GAP（缺口明确拒绝）；</li>
 *   <li>{@code seq <= lastSeq}：签名一致=DUPLICATE_SEQUENCE，位点更小=OFFSET_ROLLBACK，
 *       内容不同=SEQUENCE_CONFLICT；</li>
 *   <li>事件时间早于已观测水位超过窗口：LATE_OUTSIDE_WINDOW（只计数、不混入结果）；</li>
 *   <li>成交价落在当前最优报价价差之外：TRADE_PRICE_CONTRADICTS_QUOTE。</li>
 * </ul>
 * 所有判定只依赖事件身份、序号与事件时间，与到达时刻/线程无关，因此乱序、
 * 重放与并发写入都得到确定且一致的结论。
 */
public class SymbolAggregator {

    /**
     * 判定结论。{@code applied} 是本次真正落入聚合状态的事件
     * （连续到达时可能连同排空的缓冲事件一起返回），供上层按序写日志。
     */
    public record Decision(
            IngestionStatus status,
            RejectReason reason,
            String detail,
            List<NormalizedEvent> applied
    ) {
        static Decision accepted(String detail, List<NormalizedEvent> applied) {
            return new Decision(IngestionStatus.ACCEPTED, null, detail, applied);
        }

        static Decision held(String detail) {
            return new Decision(IngestionStatus.ACCEPTED, null, detail, List.of());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SymbolAggregator.class);

    /** 允许超前缓冲的序号宽度：跳得比它远即视为明确缺口。 */
    private static final long MAX_SEQUENCE_HOLD = 16L;

    private final SymbolAggregate state;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxOutOfOrderMillis;

    /** 已缓冲、等待缺号补齐的事件：seq -> event。 */
    private final Map<Long, NormalizedEvent> pending = new HashMap<>();
    /** 已缓冲序号的内容签名与源位点。 */
    private final Map<Long, String> heldSignatures = new HashMap<>();
    private final Map<Long, Long> heldOffsets = new HashMap<>();
    /** 已观测到的最大事件时间，作为迟到判定的水位。 */
    private long maxSeenTime = Long.MIN_VALUE;

    public SymbolAggregator(String symbol, long maxOutOfOrderMillis) {
        this.state = new SymbolAggregate(symbol);
        this.maxOutOfOrderMillis = maxOutOfOrderMillis;
    }

    /** 评估一条事件。可能立即应用、缓冲、重复或拒绝（均在锁内完成）。 */
    public Decision evaluate(NormalizedEvent event) {
        lock.lock();
        try {
            long expected = state.lastSequence() == null ? 1L : state.lastSequence() + 1;

            // 规则：旧序号 -> 重复 / 位点回退 / 内容矛盾
            if (state.lastSequence() != null && event.sequence() <= state.lastSequence()) {
                String seen = state.signatureAt(event.sequence());
                if (seen == null || !seen.equals(event.contentSignature())) {
                    return reject(RejectReason.SEQUENCE_CONFLICT,
                            "同序号 " + event.sequence() + " 内容与已接受记录矛盾或无记录: old="
                                    + seen + ", new=" + event.contentSignature());
                }
                Long seenOffset = state.offsetAt(event.sequence());
                if (seenOffset != null && event.offset() < seenOffset) {
                    return reject(RejectReason.OFFSET_ROLLBACK,
                            "同序号 " + event.sequence() + " 内容相同但源位点回退: oldOffset="
                                    + seenOffset + ", newOffset=" + event.offset());
                }
                return duplicate("同源同序号同内容重复投递: symbol=" + event.symbol()
                        + ", sequence=" + event.sequence() + ", offset=" + event.offset());
            }

            // 规则：已缓冲过的超前序号再次到达 -> 重复 / 矛盾
            if (pending.containsKey(event.sequence())) {
                String heldSig = heldSignatures.get(event.sequence());
                if (!event.contentSignature().equals(heldSig)) {
                    return reject(RejectReason.SEQUENCE_CONFLICT,
                            "缓冲序号 " + event.sequence() + " 收到不同内容: held="
                                    + heldSig + ", new=" + event.contentSignature());
                }
                Long heldOffset = heldOffsets.get(event.sequence());
                if (heldOffset != null && event.offset() < heldOffset) {
                    return reject(RejectReason.OFFSET_ROLLBACK,
                            "缓冲序号 " + event.sequence() + " 源位点回退: heldOffset="
                                    + heldOffset + ", newOffset=" + event.offset());
                }
                return duplicate("已缓冲序号 " + event.sequence() + " 的重复投递（保持缓冲）");
            }

            // 规则：超前太远 -> 明确缺口
            if (event.sequence() > expected + MAX_SEQUENCE_HOLD) {
                return reject(RejectReason.SEQUENCE_GAP,
                        "序号缺口过大: 期望 " + expected + ", 实际 " + event.sequence()
                                + ", 缓冲宽度=" + MAX_SEQUENCE_HOLD);
            }

            // 规则：迟到窗口（对已观测水位而言；未来时间不判迟到）
            if (maxSeenTime != Long.MIN_VALUE && event.eventTime() < maxSeenTime) {
                long lateness = maxSeenTime - event.eventTime();
                if (lateness > maxOutOfOrderMillis) {
                    return rejectLate("迟到 " + lateness + "ms 超过窗口 " + maxOutOfOrderMillis
                            + "ms, watermark=" + maxSeenTime + ", eventTime=" + event.eventTime());
                }
            }

            // 规则：成交价不得与已应用报价矛盾（穿价）
            if (event.type() == EventType.TRADE) {
                AggregateSnapshot snap = state.snapshot();
                if (snap.bestBidPrice() != null
                        && (event.price() < snap.bestBidPrice()
                        || event.price() > snap.bestAskPrice())) {
                    return reject(RejectReason.TRADE_PRICE_CONTRADICTS_QUOTE,
                            "成交价 " + event.price() + " 落在最优报价 ["
                                    + snap.bestBidPrice() + ", " + snap.bestAskPrice() + "] 之外");
                }
            }

            if (event.eventTime() > maxSeenTime) {
                maxSeenTime = event.eventTime();
            }

            if (event.sequence() == expected) {
                List<NormalizedEvent> applied = new ArrayList<>();
                applyToState(event, applied);
                // 连续排空缓冲区
                long next = event.sequence() + 1;
                while (pending.containsKey(next)) {
                    NormalizedEvent drained = pending.remove(next);
                    heldSignatures.remove(next);
                    heldOffsets.remove(next);
                    applyToState(drained, applied);
                    next++;
                }
                return Decision.accepted(
                        "序号连续已应用" + (applied.size() > 1
                                ? "，并排空 " + (applied.size() - 1) + " 条乱序缓冲" : ""),
                        applied);
            }

            // 超前但在缓冲宽度内：缓冲等待
            pending.put(event.sequence(), event);
            heldSignatures.put(event.sequence(), event.contentSignature());
            heldOffsets.put(event.sequence(), event.offset());
            log.info("乱序缓冲: symbol={}, seq={}, 等待 expected={}",
                    event.symbol(), event.sequence(), expected);
            return Decision.held("序号 " + event.sequence() + " 超前（等待 " + expected
                    + "），在窗口内缓冲，不计入当前聚合结果");
        } finally {
            lock.unlock();
        }
    }

    /** 重启恢复：直接按日志顺序重建状态，不做任何校验。 */
    public void restore(NormalizedEvent event) {
        lock.lock();
        try {
            state.apply(event);
            if (event.eventTime() > maxSeenTime) {
                maxSeenTime = event.eventTime();
            }
        } finally {
            lock.unlock();
        }
    }

    public AggregateSnapshot snapshot() {
        lock.lock();
        try {
            return state.snapshot();
        } finally {
            lock.unlock();
        }
    }

    /** 记录一条被拒绝事件（源位点层或序号层的拒绝统一在此计数）。 */
    public void noteRejected() {
        noteRejected(false);
    }

    /** 记录一条被拒绝事件，late=true 时同时累加迟到计数。 */
    public void noteRejected(boolean late) {
        lock.lock();
        try {
            state.incrementRejected(late);
        } finally {
            lock.unlock();
        }
    }

    /** 记录一条重复投递。 */
    public void noteDuplicate() {
        lock.lock();
        try {
            state.incrementDuplicate();
        } finally {
            lock.unlock();
        }
    }

    private void applyToState(NormalizedEvent event, List<NormalizedEvent> applied) {
        state.apply(event);
        applied.add(event);
        log.info("聚合应用: symbol={}, seq={}, type={}, eventTime={}",
                event.symbol(), event.sequence(), event.type(), event.eventTime());
    }

    private Decision reject(RejectReason reason, String detail) {
        state.incrementRejected(false);
        log.warn("拒绝: reason={}, detail={}", reason, detail);
        return new Decision(IngestionStatus.REJECTED, reason, detail, List.of());
    }

    private Decision rejectLate(String detail) {
        state.incrementRejected(true);
        log.warn("拒绝(迟到): {}", detail);
        return new Decision(IngestionStatus.REJECTED, RejectReason.LATE_OUTSIDE_WINDOW,
                detail, List.of());
    }

    private Decision duplicate(String detail) {
        state.incrementDuplicate();
        log.info("已重复: {}", detail);
        return new Decision(IngestionStatus.DUPLICATE, RejectReason.DUPLICATE_SEQUENCE,
                detail, List.of());
    }
}
