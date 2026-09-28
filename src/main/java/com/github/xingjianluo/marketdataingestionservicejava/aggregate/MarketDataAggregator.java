package com.github.xingjianluo.marketdataingestionservicejava.aggregate;

import com.github.xingjianluo.marketdataingestionservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.SymbolAggregateView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按标的维护聚合结果。
 *
 * <p>并发与时序确定规则：
 * <ul>
 *   <li>每个标的一把锁，并发写入在标的内被串行化，跨标的互不阻塞；</li>
 *   <li>去重键 {@code (sourceId, sequence)}，重复报文永不二次累加（与序号跟踪器互为双保险）；</li>
 *   <li>“最新价/最新买价/最新卖价”严格按事件时间定序：比较键
 *       {@code (eventTime, sourceId, sequence)}，到达顺序不影响结果；</li>
 *   <li>迟到判定：事件时间早于本标的已接受最大事件时间。差距 ≤ lateWindowMillis
 *       标记为 ACCEPTED_LATE_IN_WINDOW 参与聚合；超出窗口拒绝 LATE_OUT_OF_WINDOW，
 *       绝不混入结果；</li>
 *   <li>VWAP = 成交总额 / 成交总量（保留 8 位小数，HALF_UP），与到达顺序无关。</li>
 * </ul>
 */
@Component
public class MarketDataAggregator {

    private static final Logger log = LoggerFactory.getLogger(MarketDataAggregator.class);

    /** 同事件时间下“更新”的确定性比较键：(eventTime, sourceId, sequence)。 */
    private static final Comparator<NormalizedMarketData> EVENT_ORDER =
            Comparator.comparing(NormalizedMarketData::eventTime)
                    .thenComparing(NormalizedMarketData::sourceId)
                    .thenComparingLong(NormalizedMarketData::sequence);

    /** 单标的可变聚合状态。变更必须持有该对象监视器。 */
    public static final class SymbolState {
        public String symbol;
        public long acceptedCount;
        public long duplicateCount;
        public long rejectedCount;
        public long tradeCount;
        public BigDecimal totalVolume = BigDecimal.ZERO;
        public BigDecimal totalTurnover = BigDecimal.ZERO;
        public BigDecimal lastTradePrice;
        public BigDecimal highestPrice;
        public BigDecimal lowestPrice;
        public BigDecimal latestBid;
        public BigDecimal latestAsk;
        public Instant lastEventTime;
        public Instant lastIngestedAt;
        public Long lastSequence;
        public String lastSourceId;
        /** 最新成交/报价的定序键分量。 */
        public Instant lastTradeKeyTime;
        public String lastTradeKeySource;
        public long lastTradeKeySeq;
        public Instant lastBidKeyTime;
        public String lastBidKeySource;
        public long lastBidKeySeq;
        public Instant lastAskKeyTime;
        public String lastAskKeySource;
        public long lastAskKeySeq;
        /**
         * 每个源在本标的上已接受的最大事件时间。迟到判定相对“同一源”的前序水位，
         * 避免不同行情源因时钟/节奏差异互相把对方判成迟到。
         */
        public Map<String, Instant> maxEventTimeBySource = new java.util.HashMap<>();
        public Set<String> appliedKeys = ConcurrentHashMap.newKeySet();
    }

    /** apply 的判定结论。 */
    public enum ApplyStatus {
        APPLIED,
        APPLIED_LATE,
        DUPLICATE,
        LATE_OUT_OF_WINDOW
    }

    public record ApplyResult(ApplyStatus status, String message) {
    }

    private final ConcurrentHashMap<String, SymbolState> symbols = new ConcurrentHashMap<>();
    private final Clock clock;
    private final ServiceProperties properties;

    public MarketDataAggregator(Clock clock, ServiceProperties properties) {
        this.clock = clock;
        this.properties = properties;
    }

    private SymbolState stateOf(String symbol) {
        return symbols.computeIfAbsent(symbol, s -> {
            SymbolState st = new SymbolState();
            st.symbol = s;
            return st;
        });
    }

    /**
     * 尝试应用一条归一化行情。
     *
     * @return APPLIED / APPLIED_LATE（实际聚合）；DUPLICATE（已应用过）；
     *         LATE_OUT_OF_WINDOW（超出迟到窗口，被拒绝）
     */
    public ApplyResult apply(NormalizedMarketData n) {
        SymbolState st = stateOf(n.symbol());
        String dedupKey = n.sourceId() + "#" + n.sequence();
        synchronized (st) {
            if (!st.appliedKeys.add(dedupKey)) {
                st.duplicateCount++;
                return new ApplyResult(ApplyStatus.DUPLICATE,
                        "聚合层去重：" + n.symbol() + " 键 " + dedupKey + " 已应用，跳过");
            }

            long windowMillis = properties.getLateWindowMillis();
            Instant sourceHighWatermark = st.maxEventTimeBySource.get(n.sourceId());
            if (sourceHighWatermark != null && windowMillis >= 0) {
                long lagMillis = sourceHighWatermark.toEpochMilli() - n.eventTime().toEpochMilli();
                if (lagMillis > windowMillis) {
                    // 撤销去重键占用，保持状态干净
                    st.appliedKeys.remove(dedupKey);
                    st.rejectedCount++;
                    return new ApplyResult(ApplyStatus.LATE_OUT_OF_WINDOW,
                            "迟到超出窗口：" + n.symbol() + " source=" + n.sourceId()
                                    + " eventTime=" + n.eventTime()
                                    + "，同源已接受最大事件时间=" + sourceHighWatermark
                                    + "，迟到 " + lagMillis + "ms > 窗口 " + windowMillis + "ms");
                }
                if (lagMillis > 0) {
                    applyLocked(st, n);
                    return new ApplyResult(ApplyStatus.APPLIED_LATE,
                            "窗口内迟到并按事件时间归位：" + n.symbol()
                                    + " source=" + n.sourceId()
                                    + " eventTime=" + n.eventTime() + "，落后同源水位 "
                                    + lagMillis + "ms ≤ 窗口 " + windowMillis + "ms");
                }
            }
            applyLocked(st, n);
            return new ApplyResult(ApplyStatus.APPLIED,
                    "正常聚合：" + n.symbol() + " eventTime=" + n.eventTime());
        }
    }

    private void applyLocked(SymbolState st, NormalizedMarketData n) {
        st.acceptedCount++;
        Instant now = Instant.now(clock);
        st.lastIngestedAt = now;
        if (st.lastEventTime == null || n.eventTime().isAfter(st.lastEventTime)) {
            st.lastEventTime = n.eventTime();
        }
        st.maxEventTimeBySource.merge(n.sourceId(), n.eventTime(),
                (old, incoming) -> incoming.isAfter(old) ? incoming : old);
        st.lastSequence = n.sequence();
        st.lastSourceId = n.sourceId();

        switch (n.type()) {
            case TRADE -> applyTradeLocked(st, n);
            case QUOTE -> applyQuoteLocked(st, n);
        }
        log.debug("已聚合 {}: accepted={} volume={}", st.symbol, st.acceptedCount, st.totalVolume);
    }

    private void applyTradeLocked(SymbolState st, NormalizedMarketData n) {
        st.tradeCount++;
        BigDecimal turnover = n.price().multiply(n.size());
        st.totalVolume = st.totalVolume.add(n.size());
        st.totalTurnover = st.totalTurnover.add(turnover);
        if (st.highestPrice == null || n.price().compareTo(st.highestPrice) > 0) {
            st.highestPrice = n.price();
        }
        if (st.lowestPrice == null || n.price().compareTo(st.lowestPrice) < 0) {
            st.lowestPrice = n.price();
        }
        // 最新成交价按 (eventTime, sourceId, sequence) 定序，与到达顺序无关
        if (st.lastTradeKeyTime == null || isNewer(n, st.lastTradeKeyTime,
                st.lastTradeKeySource, st.lastTradeKeySeq)) {
            st.lastTradePrice = n.price();
            st.lastTradeKeyTime = n.eventTime();
            st.lastTradeKeySource = n.sourceId();
            st.lastTradeKeySeq = n.sequence();
        }
    }

    private void applyQuoteLocked(SymbolState st, NormalizedMarketData n) {
        switch (n.side()) {
            case "BID" -> {
                if (st.lastBidKeyTime == null || isNewer(n, st.lastBidKeyTime,
                        st.lastBidKeySource, st.lastBidKeySeq)) {
                    st.latestBid = n.price();
                    st.lastBidKeyTime = n.eventTime();
                    st.lastBidKeySource = n.sourceId();
                    st.lastBidKeySeq = n.sequence();
                }
            }
            case "ASK" -> {
                if (st.lastAskKeyTime == null || isNewer(n, st.lastAskKeyTime,
                        st.lastAskKeySource, st.lastAskKeySeq)) {
                    st.latestAsk = n.price();
                    st.lastAskKeyTime = n.eventTime();
                    st.lastAskKeySource = n.sourceId();
                    st.lastAskKeySeq = n.sequence();
                }
            }
            default -> throw new IllegalStateException("归一化后方向仍非法: " + n.side());
        }
    }

    private static boolean isNewer(NormalizedMarketData n, Instant oldTime,
                                   String oldSource, long oldSeq) {
        return EVENT_ORDER.compare(n, keyOf(oldTime, oldSource, oldSeq)) > 0;
    }

    private static NormalizedMarketData keyOf(Instant time, String source, long seq) {
        return new NormalizedMarketData(source, seq, null, "", time,
                BigDecimal.ZERO, BigDecimal.ZERO, "");
    }

    /** 供接入服务在归一化/序号阶段拒绝时累计每标的拒绝数（symbol 可能未知，忽略）。 */
    public void countRejected(String symbol) {
        if (symbol == null) {
            return;
        }
        SymbolState st = stateOf(symbol);
        synchronized (st) {
            st.rejectedCount++;
        }
    }

    public void countDuplicate(String symbol) {
        if (symbol == null) {
            return;
        }
        SymbolState st = stateOf(symbol);
        synchronized (st) {
            st.duplicateCount++;
        }
    }

    public Optional<SymbolAggregateView> view(String symbol) {
        SymbolState st = symbols.get(symbol);
        if (st == null) {
            return Optional.empty();
        }
        synchronized (st) {
            return Optional.of(toView(st));
        }
    }

    public List<SymbolAggregateView> allViews() {
        List<SymbolAggregateView> result = new ArrayList<>();
        for (String symbol : new ArrayList<>(symbols.keySet())) {
            view(symbol).ifPresent(result::add);
        }
        result.sort(Comparator.comparing(SymbolAggregateView::symbol));
        return result;
    }

    private static SymbolAggregateView toView(SymbolState st) {
        BigDecimal vwap = st.totalVolume.signum() > 0
                ? st.totalTurnover.divide(st.totalVolume, 8, RoundingMode.HALF_UP)
                : null;
        return new SymbolAggregateView(
                st.symbol, st.acceptedCount, st.duplicateCount, st.rejectedCount,
                st.tradeCount, st.totalVolume, st.lastTradePrice,
                st.highestPrice, st.lowestPrice, vwap,
                st.latestBid, st.latestAsk, st.lastEventTime, st.lastIngestedAt,
                st.lastSequence, st.lastSourceId);
    }

    /** 导出可持久化快照。 */
    public Map<String, com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.SymbolSnapshot> exportSnapshot() {
        Map<String, com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.SymbolSnapshot> out =
                new java.util.LinkedHashMap<>();
        for (Map.Entry<String, SymbolState> e : symbols.entrySet()) {
            SymbolState st = e.getValue();
            synchronized (st) {
                out.put(e.getKey(),
                        new com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.SymbolSnapshot(
                                st.symbol, st.acceptedCount, st.duplicateCount, st.rejectedCount,
                                st.tradeCount,
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.totalVolume),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.totalTurnover),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.lastTradePrice),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.highestPrice),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.lowestPrice),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.latestBid),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.latestAsk),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.lastEventTime),
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.lastTradeKeyTime),
                                st.lastTradeKeySource, st.lastTradeKeySeq,
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.lastBidKeyTime),
                                st.lastBidKeySource, st.lastBidKeySeq,
                                com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(st.lastAskKeyTime),
                                st.lastAskKeySource, st.lastAskKeySeq,
                                st.lastSequence, st.lastSourceId,
                                st.maxEventTimeBySource.entrySet().stream()
                                        .collect(java.util.stream.Collectors.toMap(
                                                Map.Entry::getKey,
                                                en -> com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.s(en.getValue()))),
                                List.copyOf(st.appliedKeys)));
            }
        }
        return out;
    }

    /** 从快照恢复（重启重放前调用）。 */
    public void importSnapshot(Map<String, com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.SymbolSnapshot> snapshot) {
        symbols.clear();
        snapshot.forEach((symbol, s) -> {
            SymbolState st = new SymbolState();
            st.symbol = symbol;
            st.acceptedCount = s.acceptedCount();
            st.duplicateCount = s.duplicateCount();
            st.rejectedCount = s.rejectedCount();
            st.tradeCount = s.tradeCount();
            st.totalVolume = com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.bd(s.totalVolume());
            st.totalTurnover = com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.bd(s.totalTurnover());
            st.lastTradePrice = com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.bd(s.lastTradePrice());
            st.highestPrice = com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.bd(s.highestPrice());
            st.lowestPrice = com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.bd(s.lowestPrice());
            st.latestBid = com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.bd(s.latestBid());
            st.latestAsk = com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayStateStore.bd(s.latestAsk());
            st.lastEventTime = parseInstant(s.lastEventTime());
            st.lastTradeKeyTime = parseInstant(s.lastTradeKeyTime());
            st.lastTradeKeySource = s.lastTradeKeySource();
            st.lastTradeKeySeq = s.lastTradeKeySeq();
            st.lastBidKeyTime = parseInstant(s.lastBidKeyTime());
            st.lastBidKeySource = s.lastBidKeySource();
            st.lastBidKeySeq = s.lastBidKeySeq();
            st.lastAskKeyTime = parseInstant(s.lastAskKeyTime());
            st.lastAskKeySource = s.lastAskKeySource();
            st.lastAskKeySeq = s.lastAskKeySeq();
            st.lastSequence = s.lastSequence();
            st.lastSourceId = s.lastSourceId();
            if (s.maxEventTimeBySource() != null) {
                s.maxEventTimeBySource().forEach((src, ts) ->
                        st.maxEventTimeBySource.put(src, java.time.Instant.parse(ts)));
            }
            st.appliedKeys.addAll(s.appliedKeys());
            symbols.put(symbol, st);
        });
    }

    private static java.time.Instant parseInstant(String s) {
        return s == null ? null : java.time.Instant.parse(s);
    }
}
