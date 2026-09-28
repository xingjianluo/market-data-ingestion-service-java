package com.github.xingjianluo.marketdataingestionservicejava.ingest;

import com.github.xingjianluo.marketdataingestionservicejava.aggregation.AggregationService;
import com.github.xingjianluo.marketdataingestionservicejava.journal.EventJournal;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizeResult;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 接入管线：归一化校验 → 源位点/标的序号去重与聚合 → 仅追加日志。
 *
 * <p>只有 ACCEPTED 才写日志；REJECTED/DUPLICATE 不写，因此重放恢复时
 * 不会把历史拒绝或重复数据重新计入。启动时（{@link ApplicationReadyEvent}）
 * 把日志按序重放一遍重建内存状态。
 */
@Service
public class MarketDataIngestor {

    private static final Logger log = LoggerFactory.getLogger(MarketDataIngestor.class);

    private final EventNormalizer normalizer;
    private final AggregationService aggregationService;
    private final EventJournal journal;
    private final AtomicLong replayedCount = new AtomicLong();

    public MarketDataIngestor(EventNormalizer normalizer,
                              AggregationService aggregationService,
                              EventJournal journal) {
        this.normalizer = normalizer;
        this.aggregationService = aggregationService;
        this.journal = journal;
    }

    /** 启动后从本地日志重放恢复。 */
    @EventListener(ApplicationReadyEvent.class)
    public void recover() {
        var history = journal.replayAll();
        for (NormalizedEvent event : history) {
            aggregationService.replay(event);
        }
        replayedCount.set(history.size());
        log.info("重放恢复完成: 共重建 {} 条已接受事件", history.size());
    }

    public long replayedCount() {
        return replayedCount.get();
    }

    public IngestionOutcome ingest(RawMarketEvent raw) {
        log.info("收到行情输入: {}", raw);

        NormalizeResult result = normalizer.normalize(raw);
        if (!result.valid()) {
            IngestionOutcome outcome = rejected(result.reason(), raw,
                    "归一化阶段拒绝");
            log.info("判定结论: input={}, outcome={}", raw, outcome);
            return outcome;
        }

        NormalizedEvent event = result.event();
        AggregationService.Result processResult;
        try {
            processResult = aggregationService.process(event);
        } catch (RuntimeException ex) {
            log.error("聚合处理异常，保守拒绝: input={}", raw, ex);
            return new IngestionOutcome(
                    IngestionStatus.REJECTED, null,
                    raw.sourceId(), raw.offset(), raw.symbol(),
                    raw.sequence(), raw.eventTime(),
                    "内部错误: " + ex.getMessage());
        }
        IngestionOutcome outcome = processResult.outcome();

        if (outcome.status() == IngestionStatus.ACCEPTED) {
            // 立即应用的事件 + 因缺号补齐而排空的乱序缓冲事件，按应用顺序持久化
            for (NormalizedEvent applied : processResult.applied()) {
                try {
                    journal.append(applied);
                } catch (RuntimeException ex) {
                    log.error("事件已聚合但写日志失败（重启后可能丢失）: {}", applied, ex);
                    return new IngestionOutcome(
                            IngestionStatus.REJECTED, null,
                            applied.sourceId(), applied.offset(), applied.symbol(),
                            applied.sequence(), applied.eventTime(),
                            "日志写入失败: " + ex.getMessage());
                }
            }
        }
        log.info("判定结论: input={}, outcome={}", raw, outcome);
        return outcome;
    }

    private IngestionOutcome rejected(RejectReason reason, RawMarketEvent raw,
                                      String detail) {
        return new IngestionOutcome(
                IngestionStatus.REJECTED, reason,
                raw == null ? null : raw.sourceId(),
                raw == null ? null : raw.offset(),
                raw == null ? null : raw.symbol(),
                raw == null ? null : raw.sequence(),
                raw == null ? null : raw.eventTime(),
                detail);
    }
}
