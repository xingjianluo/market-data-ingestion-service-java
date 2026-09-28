package com.github.xingjianluo.marketdataingestionservicejava.service;

import com.github.xingjianluo.marketdataingestionservicejava.aggregate.MarketDataAggregator;
import com.github.xingjianluo.marketdataingestionservicejava.model.AcceptedFlag;
import com.github.xingjianluo.marketdataingestionservicejava.model.DuplicateReason;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import com.github.xingjianluo.marketdataingestionservicejava.model.SourceStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.SymbolAggregateView;
import com.github.xingjianluo.marketdataingestionservicejava.normalize.MarketDataNormalizer;
import com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore;
import com.github.xingjianluo.marketdataingestionservicejava.replay.SourceSequenceTracker;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 接入门面：串联 归一化 → 序号/缺口判定 → 聚合 → 位点推进与持久化。
 *
 * <p>对一条输入，对外只给出三种可解释结论：
 * <ul>
 *   <li>ACCEPTED（flag=ACCEPTED / ACCEPTED_LATE_IN_WINDOW）；</li>
 *   <li>REJECTED（携带 {@link RejectReason}，含 SEQUENCE_GAP / LATE_OUT_OF_WINDOW）；</li>
 *   <li>DUPLICATE（携带 {@link DuplicateReason}，区分完全重复 / 同序号矛盾 / 位点已提交）。</li>
 * </ul>
 */
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final MarketDataNormalizer normalizer;
    private final SourceSequenceTracker sequenceTracker;
    private final MarketDataAggregator aggregator;
    private final ReplayStateStore stateStore;

    public IngestionService(MarketDataNormalizer normalizer,
                            SourceSequenceTracker sequenceTracker,
                            MarketDataAggregator aggregator,
                            ReplayStateStore stateStore) {
        this.normalizer = normalizer;
        this.sequenceTracker = sequenceTracker;
        this.aggregator = aggregator;
        this.stateStore = stateStore;
    }

    /** 启动时从本地快照恢复聚合与序号/位点状态。 */
    @PostConstruct
    public void restore() {
        stateStore.load().ifPresent(snapshot -> {
            sequenceTracker.importSnapshot(snapshot.sources());
            aggregator.importSnapshot(snapshot.symbols());
            log.info("重启恢复完成：恢复源 {} 个、标的 {} 个",
                    snapshot.sources().size(), snapshot.symbols().size());
        });
    }

    /**
     * 实时接入一条报文。
     */
    public IngestionOutcome ingest(RawMarketData raw) {
        return process(raw, null);
    }

    /**
     * 接入一条报文并附带其投递位点 offset（重放路径）。
     *
     * @param offset 该报文在源脚本中的位点；null 表示实时投递。
     *               offset ≤ 已提交位点 → DUPLICATE(ALREADY_COMMITTED_OFFSET)；
     *               已提交位点之后若出现序号倒退，由序号跟踪器区分缺口/重复。
     */
    public IngestionOutcome ingestAt(RawMarketData raw, long offset) {
        return process(raw, offset);
    }

    private IngestionOutcome process(RawMarketData raw, Long offset) {
        log.info("接入输入 raw={} offset={}", raw, offset);

        // 1) 重放位点幂等：位点已提交过，直接判重，不再重复处理
        if (raw != null && offset != null) {
            long committed = sequenceTracker.committedOffset(raw.sourceId());
            if (offset <= committed) {
                log.info("判定 DUPLICATE(ALREADY_COMMITTED_OFFSET): source={} offset={} committed={}",
                        raw.sourceId(), offset, committed);
                aggregator.countDuplicate(raw.symbol());
                return IngestionOutcome.duplicate(DuplicateReason.ALREADY_COMMITTED_OFFSET,
                        "位点已提交，重放不重复处理：source=" + raw.sourceId()
                                + " offset=" + offset + " ≤ committed=" + committed,
                        raw.symbol());
            }
        }

        // 2) 归一化校验
        IngestionOutcome normalized = normalizer.normalize(raw);
        if (normalized.status() != IngestionStatus.ACCEPTED) {
            log.info("判定 {}: {}", normalized.status(), normalized.message());
            if (normalized.symbol() != null) {
                aggregator.countRejected(normalized.symbol());
            }
            persistQuietly();
            return normalized;
        }
        NormalizedMarketData n = normalized.normalized();

        // 3) 序号/缺口原子判定（指纹 = 归一化后全部业务字段）
        String fingerprint = fingerprint(n);
        SourceSequenceTracker.Decision decision =
                sequenceTracker.acceptIfNew(n.sourceId(), n.sequence(), fingerprint);
        switch (decision.type()) {
            case GAP -> {
                log.info("判定 REJECTED(SEQUENCE_GAP): {}", decision.message());
                aggregator.countRejected(n.symbol());
                persistQuietly();
                return IngestionOutcome.rejected(RejectReason.SEQUENCE_GAP,
                        decision.message(), n.symbol());
            }
            case DUPLICATE_SAME -> {
                log.info("判定 DUPLICATE(SAME_SEQUENCE_SAME_PAYLOAD): {}", decision.message());
                aggregator.countDuplicate(n.symbol());
                if (offset != null) {
                    sequenceTracker.commitOffset(n.sourceId(), offset);
                }
                persistQuietly();
                return IngestionOutcome.duplicate(DuplicateReason.SAME_SEQUENCE_SAME_PAYLOAD,
                        decision.message(), n.symbol());
            }
            case DUPLICATE_CONTRADICTORY -> {
                log.info("判定 REJECTED(CONTRADICTORY_RECORD): {}", decision.message());
                aggregator.countRejected(n.symbol());
                if (offset != null) {
                    sequenceTracker.commitOffset(n.sourceId(), offset);
                }
                persistQuietly();
                return IngestionOutcome.rejected(RejectReason.CONTRADICTORY_RECORD,
                        decision.message(), n.symbol());
            }
            case NEW -> {
                // 继续聚合
            }
        }

        // 4) 按标的聚合（内部含窗口判定与二次去重）
        MarketDataAggregator.ApplyResult applied = aggregator.apply(n);
        IngestionOutcome outcome;
        switch (applied.status()) {
            case APPLIED -> {
                outcome = IngestionOutcome.accepted(n, AcceptedFlag.ACCEPTED);
            }
            case APPLIED_LATE -> {
                outcome = IngestionOutcome.accepted(n, AcceptedFlag.ACCEPTED_LATE_IN_WINDOW);
            }
            case LATE_OUT_OF_WINDOW -> {
                log.info("判定 REJECTED(LATE_OUT_OF_WINDOW): {}", applied.message());
                persistQuietly();
                return IngestionOutcome.rejected(RejectReason.LATE_OUT_OF_WINDOW,
                        applied.message(), n.symbol());
            }
            case DUPLICATE -> {
                log.info("判定 DUPLICATE(SAME_SEQUENCE_SAME_PAYLOAD)@聚合层: {}", applied.message());
                if (offset != null) {
                    sequenceTracker.commitOffset(n.sourceId(), offset);
                }
                persistQuietly();
                return IngestionOutcome.duplicate(DuplicateReason.SAME_SEQUENCE_SAME_PAYLOAD,
                        applied.message(), n.symbol());
            }
            default -> throw new IllegalStateException("未知聚合结论: " + applied.status());
        }
        log.info("判定 {}({}): {}", outcome.status(), outcome.flag(), outcome.message());

        // 5) 位点推进并持久化（崩溃恢复不丢不重）
        if (offset != null) {
            sequenceTracker.commitOffset(n.sourceId(), offset);
        }
        persistQuietly();
        return outcome;
    }

    /** 归一化载荷指纹：序号相同但任一业务字段不同即视为相互矛盾。 */
    public static String fingerprint(NormalizedMarketData n) {
        return n.type() + "|" + n.symbol() + "|" + n.eventTime() + "|"
                + n.price().stripTrailingZeros().toPlainString() + "|"
                + n.size().stripTrailingZeros().toPlainString() + "|" + n.side();
    }

    private void persistQuietly() {
        try {
            persist();
        } catch (IOException e) {
            log.error("状态持久化失败，重启后需重放恢复", e);
        }
    }

    /** 构造并落盘当前完整快照。 */
    public ReplayStateStore.Snapshot persist() throws IOException {
        ReplayStateStore.Snapshot snapshot = new ReplayStateStore.Snapshot(
                Instant.now().toString(),
                sequenceTracker.exportSnapshot(),
                aggregator.exportSnapshot());
        stateStore.save(snapshot);
        return snapshot;
    }

    public Optional<SymbolAggregateView> aggregateOf(String symbol) {
        return aggregator.view(symbol);
    }

    public List<SymbolAggregateView> allAggregates() {
        return aggregator.allViews();
    }

    public Optional<SourceStatus> sourceStatus(String sourceId) {
        return Optional.ofNullable(sequenceTracker.status(sourceId));
    }

    public List<SourceStatus> allSourceStatus() {
        return sequenceTracker.allStatus();
    }
}
