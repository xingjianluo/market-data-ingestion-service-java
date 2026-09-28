package com.github.xingjianluo.marketdataingestionservicejava.replay;

import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.ReplayStartResult;
import com.github.xingjianluo.marketdataingestionservicejava.model.ReplayVerdict;
import com.github.xingjianluo.marketdataingestionservicejava.service.IngestionService;
import com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 重放协调器：从指定位点开始重放本地脚本。
 *
 * <p>位点规则（结论可区分、不丢不重）：
 * <ul>
 *   <li>{@code fromOffset > committedOffset}：正常重放（OK）；若中间跳过了位点，
 *       序号跟踪器会对缺失序号给出 SEQUENCE_GAP；</li>
 *   <li>{@code fromOffset <= committedOffset}：位点回退（OFFSET_ROLLBACK），
 *       仍允许逐帧重放，但已提交位点的报文统一判 DUPLICATE(ALREADY_COMMITTED_OFFSET)，
 *       不会重复累加；</li>
 *   <li>从未提交过的源 committed=-1，fromOffset=0 即全新消费。</li>
 * </ul>
 */
@Component
public class ReplayCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ReplayCoordinator.class);

    private final IngestionService ingestionService;
    private final LocalMarketDataSource dataSource;
    private final SourceSequenceTracker sequenceTracker;

    public ReplayCoordinator(IngestionService ingestionService,
                             LocalMarketDataSource dataSource,
                             SourceSequenceTracker sequenceTracker) {
        this.ingestionService = ingestionService;
        this.dataSource = dataSource;
        this.sequenceTracker = sequenceTracker;
    }

    /**
     * 校验重放起点。
     */
    public ReplayStartResult startReplay(String sourceId, long fromOffset) {
        long committed = sequenceTracker.committedOffset(sourceId);
        if (committed >= 0 && fromOffset <= committed) {
            String msg = "位点回退：source=" + sourceId + " fromOffset=" + fromOffset
                    + " ≤ committed=" + committed
                    + "；已提交位点的报文将判重而不累加";
            log.info(msg);
            return new ReplayStartResult(sourceId, ReplayVerdict.OFFSET_ROLLBACK,
                    fromOffset, committed, msg);
        }
        String msg = "重放起点正常：source=" + sourceId + " fromOffset=" + fromOffset
                + "，committed=" + committed;
        log.info(msg);
        return new ReplayStartResult(sourceId, ReplayVerdict.OK, fromOffset,
                committed < 0 ? null : committed, msg);
    }

    /**
     * 重放指定场景中 {@code [fromOffset, end)} 的报文，返回首帧位点结论与
     * 每帧的接入判定。
     *
     * @param scenarioName 本地脚本名（见 {@link LocalMarketDataSource#batch(String)}）
     */
    public ReplaySession replay(String scenarioName, long fromOffset) {
        LocalMarketDataSource.Batch batch = dataSource.batch(scenarioName);
        List<RawMarketData> records = batch.fromOffset(fromOffset);
        String sourceId = records.isEmpty()
                ? batch.records().stream().findFirst().map(RawMarketData::sourceId).orElse("UNKNOWN")
                : records.get(0).sourceId();

        ReplayStartResult start = startReplay(sourceId, fromOffset);
        List<IngestionOutcome> outcomes = new ArrayList<>();
        long offset = fromOffset;
        for (RawMarketData raw : records) {
            outcomes.add(ingestionService.ingestAt(raw, offset));
            offset++;
        }
        log.info("重放完成 scenario={} fromOffset={} frames={} verdict={}",
                scenarioName, fromOffset, outcomes.size(), start.verdict());
        return new ReplaySession(start, outcomes);
    }

    /** 一次重放会话：起点结论 + 逐帧结果。 */
    public record ReplaySession(ReplayStartResult start, List<IngestionOutcome> outcomes) {
    }
}
