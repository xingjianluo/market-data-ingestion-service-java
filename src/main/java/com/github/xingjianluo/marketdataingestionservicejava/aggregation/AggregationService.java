package com.github.xingjianluo.marketdataingestionservicejava.aggregation;

import com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.AggregateSnapshot;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 聚合服务：按标的维护 {@link SymbolAggregator}，并维护每个源的位点状态。
 *
 * <p>源位点层（跨标的）与标的序号层是两套独立但同构的“已见集合 + 高水位”模型，
 * 都能容忍窗口内乱序、并把重复与回退区分开：
 * <ul>
 *   <li>位点已见过且内容一致 = {@code DUPLICATE_OFFSET}（已重复，幂等）；</li>
 *   <li>位点已见过但内容不同 = {@code OFFSET_ROLLBACK}（同位点篡改，拒绝）；</li>
 *   <li>位点未见过、但小于已见最小期望（落后超过缓冲宽度）= {@code OFFSET_ROLLBACK}（回退，拒绝）；</li>
 *   <li>位点超前于高水位超过缓冲宽度 = {@code SEQUENCE_GAP}（跳空，拒绝）；</li>
 *   <li>窗口内的乱序位点正常接受（可能在标的层被缓冲）。</li>
 * </ul>
 * “缓冲中”的事件同样占用其位点，因此乱序补齐不会被误判为回退或跳空。
 * 全部检查在“源锁 -> 标的锁”的固定加锁顺序下进行，并发写入也不会重复计价或错乱。
 */
@Service
public class AggregationService {

    /**
     * 处理结果：对外结论 + 本次真正落入聚合状态的事件（含乱序排空），
     * 调用方据此按应用顺序写日志。
     */
    public record Result(IngestionOutcome outcome, List<NormalizedEvent> applied) {
    }

    /** 与标的序号缓冲宽度一致：源位点允许的乱序/缺口宽度。 */
    private static final long MAX_OFFSET_HOLD = 16L;

    private static final Logger log = LoggerFactory.getLogger(AggregationService.class);

    /** 单个源的位点状态。 */
    private static final class SourceState {
        /** 已见位点高水位（含缓冲中）。 */
        long maxOffset = Long.MIN_VALUE;
        /** 已见位点 -> 内容签名。 */
        final Map<Long, String> signatures = new ConcurrentHashMap<>();
    }

    private final long maxOutOfOrderMillis;
    private final Map<String, SymbolAggregator> aggregators = new ConcurrentHashMap<>();
    private final Map<String, SourceState> sources = new ConcurrentHashMap<>();
    private final Map<String, Object> sourceLocks = new ConcurrentHashMap<>();

    public AggregationService(MarketDataProperties properties) {
        this.maxOutOfOrderMillis = properties.getMaxOutOfOrderMillis();
    }

    /**
     * 在线处理：源位点检查 -> 标的序号/时序检查。
     * {@link Result#applied()} 为本次真正落入聚合状态的事件（立即应用 + 连续排空）。
     */
    public Result process(NormalizedEvent event) {
        Object sLock = sourceLocks.computeIfAbsent(event.sourceId(), k -> new Object());
        SymbolAggregator aggregator = aggregators.computeIfAbsent(event.symbol(),
                s -> new SymbolAggregator(s, maxOutOfOrderMillis));

        // 固定加锁顺序：源锁 -> 标的锁
        synchronized (sLock) {
            SourceState src = sources.computeIfAbsent(event.sourceId(), k -> new SourceState());

            // ---- 源位点层 ----
            if (src.maxOffset != Long.MIN_VALUE) {
                String seenSig = src.signatures.get(event.offset());

                if (seenSig != null) {
                    if (seenSig.equals(event.contentSignature())) {
                        aggregator.noteDuplicate();
                        return new Result(duplicate(event, RejectReason.DUPLICATE_OFFSET,
                                "源 " + event.sourceId() + " 位点 " + event.offset()
                                        + " 已见过且内容一致，重复投递（幂等忽略）"), List.of());
                    }
                    aggregator.noteRejected();
                    return new Result(rejected(event, RejectReason.OFFSET_ROLLBACK,
                            "源 " + event.sourceId() + " 位点 " + event.offset()
                                    + " 已见过但内容不同（同位点篡改）: old=" + seenSig
                                    + ", new=" + event.contentSignature()), List.of());
                }

                // 未见过的位点：超前太多 => 跳空；落后于可接受下界 => 回退
                if (event.offset() > src.maxOffset + MAX_OFFSET_HOLD) {
                    aggregator.noteRejected();
                    return new Result(rejected(event, RejectReason.SEQUENCE_GAP,
                            "源 " + event.sourceId() + " 位点跳空: maxOffset=" + src.maxOffset
                                    + ", incoming=" + event.offset() + ", 缓冲宽度=" + MAX_OFFSET_HOLD),
                            List.of());
                }
                long lowerBound = Math.max(1L, src.maxOffset - MAX_OFFSET_HOLD);
                if (event.offset() < lowerBound) {
                    aggregator.noteRejected();
                    return new Result(rejected(event, RejectReason.OFFSET_ROLLBACK,
                            "源 " + event.sourceId() + " 位点回退超出缓冲宽度: maxOffset="
                                    + src.maxOffset + ", incoming=" + event.offset()
                                    + ", 下界=" + lowerBound), List.of());
                }
            }

            // ---- 标的序号/时序层 ----
            SymbolAggregator.Decision decision = aggregator.evaluate(event);
            if (decision.status() != IngestionStatus.ACCEPTED) {
                IngestionOutcome outcome = decision.status() == IngestionStatus.DUPLICATE
                        ? duplicate(event, decision.reason(), decision.detail())
                        : rejected(event, decision.reason(), decision.detail());
                return new Result(outcome, List.of());
            }

            // ---- 接受：占用源位点（含缓冲中的事件） ----
            src.signatures.put(event.offset(), event.contentSignature());
            if (event.offset() > src.maxOffset) {
                src.maxOffset = event.offset();
            }
            log.info("接受: source={}, offset={}, symbol={}, seq={}, 本次落地={}",
                    event.sourceId(), event.offset(), event.symbol(),
                    event.sequence(), decision.applied().size());

            IngestionOutcome outcome = new IngestionOutcome(
                    IngestionStatus.ACCEPTED, null,
                    event.sourceId(), event.offset(), event.symbol(),
                    event.sequence(), event.eventTime(), decision.detail());
            return new Result(outcome, decision.applied());
        }
    }

    /**
     * 重启恢复：把日志中的已接受事件按序直接重建，不做任何拒绝/重复判定，
     * 因此“同一批数据被重复投递或回退重放”不会造成重复累加或丢失。
     */
    public void replay(NormalizedEvent event) {
        SymbolAggregator aggregator = aggregators.computeIfAbsent(event.symbol(),
                s -> new SymbolAggregator(s, maxOutOfOrderMillis));
        Object sLock = sourceLocks.computeIfAbsent(event.sourceId(), k -> new Object());
        synchronized (sLock) {
            SourceState src = sources.computeIfAbsent(event.sourceId(), k -> new SourceState());
            aggregator.restore(event);
            src.signatures.put(event.offset(), event.contentSignature());
            if (event.offset() > src.maxOffset) {
                src.maxOffset = event.offset();
            }
        }
    }

    public AggregateSnapshot snapshot(String symbol) {
        SymbolAggregator aggregator = aggregators.get(symbol);
        return aggregator == null ? null : aggregator.snapshot();
    }

    public List<AggregateSnapshot> snapshots() {
        List<AggregateSnapshot> list = new ArrayList<>();
        for (SymbolAggregator aggregator : aggregators.values()) {
            list.add(aggregator.snapshot());
        }
        list.sort(Comparator.comparing(AggregateSnapshot::symbol));
        return list;
    }

    private IngestionOutcome rejected(NormalizedEvent event, RejectReason reason, String detail) {
        log.warn("拒绝: source={}, offset={}, symbol={}, seq={}, reason={}, detail={}",
                event.sourceId(), event.offset(), event.symbol(), event.sequence(),
                reason, detail);
        return new IngestionOutcome(
                IngestionStatus.REJECTED, reason,
                event.sourceId(), event.offset(), event.symbol(),
                event.sequence(), event.eventTime(), detail);
    }

    private IngestionOutcome duplicate(NormalizedEvent event, RejectReason reason, String detail) {
        log.info("已重复: source={}, offset={}, symbol={}, seq={}, reason={}, detail={}",
                event.sourceId(), event.offset(), event.symbol(), event.sequence(),
                reason, detail);
        return new IngestionOutcome(
                IngestionStatus.DUPLICATE, reason,
                event.sourceId(), event.offset(), event.symbol(),
                event.sequence(), event.eventTime(), detail);
    }
}
