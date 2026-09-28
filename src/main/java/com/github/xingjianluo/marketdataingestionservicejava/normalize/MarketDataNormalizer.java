package com.github.xingjianluo.marketdataingestionservicejava.normalize;

import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.MarketDataType;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * 原始报文归一化：把外部字符串报文校验并转换为 {@link NormalizedMarketData}。
 *
 * <p>校验顺序固定（保证同一输入永远得到同一个拒绝原因，结论可解释）：
 * <ol>
 *   <li>源标识/序号存在且为正；</li>
 *   <li>行情类型合法（TRADE/QUOTE），方向与类型匹配；</li>
 *   <li>标的非空且在注册表内；</li>
 *   <li>时间戳可解析、非远古、相对当前时钟不超过 {@link #FUTURE_TOLERANCE}；</li>
 *   <li>价格/数量可解析且严格为正；</li>
 *   <li>字段间一致性：报价方向 BID 不得高于 ASK 等跨字段矛盾在聚合层按同序号判定，
 *       本层只处理单条报文内部矛盾（未知方向等）。</li>
 * </ol>
 * 任何失败都返回 REJECTED 并携带具体原因，绝不静默丢弃。
 */
@Component
public class MarketDataNormalizer {

    private static final Logger log = LoggerFactory.getLogger(MarketDataNormalizer.class);

    /** 事件时间允许领先当前时钟的最大值（防止时钟漂移/伪造未来时间）。 */
    static final long FUTURE_TOLERANCE_MILLIS = 60_000L;

    /** 早于该时间的时间戳视为非法（远早于系统有意义的时间范围）。 */
    private static final Instant MIN_VALID_TIME = Instant.parse("2000-01-01T00:00:00Z");

    private static final Set<String> TRADE_SIDES = Set.of("BUY", "SELL");
    private static final Set<String> QUOTE_SIDES = Set.of("BID", "ASK");

    private final InstrumentRegistry instrumentRegistry;
    private final Clock clock;

    public MarketDataNormalizer(InstrumentRegistry instrumentRegistry, Clock clock) {
        this.instrumentRegistry = instrumentRegistry;
        this.clock = clock;
    }

    /**
     * 归一化一条原始报文。
     *
     * @return 一定非空：要么 ACCEPTED（含归一化模型），要么 REJECTED（含原因与依据）
     */
    public IngestionOutcome normalize(RawMarketData raw) {
        log.debug("归一化输入: {}", raw);
        if (raw == null) {
            return reject(RejectReason.MISSING_FIELD, "报文为空", null);
        }

        String symbol = raw.symbol() == null ? null : raw.symbol().trim();

        if (isBlank(raw.sourceId())) {
            return reject(RejectReason.MISSING_FIELD, "缺少行情源标识 sourceId", symbol);
        }
        if (raw.sequence() == null || raw.sequence() <= 0) {
            return reject(RejectReason.MISSING_FIELD,
                    "缺少或非法的序号 sequence=" + raw.sequence(), symbol);
        }
        if (isBlank(raw.type())) {
            return reject(RejectReason.MISSING_FIELD, "缺少行情类型 type", symbol);
        }
        MarketDataType type;
        try {
            type = MarketDataType.valueOf(raw.type().trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return reject(RejectReason.INVALID_PAYLOAD,
                    "未知行情类型 type=" + raw.type(), symbol);
        }
        if (isBlank(symbol)) {
            return reject(RejectReason.MISSING_FIELD, "缺少标的 symbol", symbol);
        }
        if (isBlank(raw.side())) {
            return reject(RejectReason.MISSING_FIELD, "缺少成交/报价方向 side", symbol);
        }
        String side = raw.side().trim().toUpperCase();
        if (type == MarketDataType.TRADE && !TRADE_SIDES.contains(side)) {
            return reject(RejectReason.CONTRADICTORY_RECORD,
                    "TRADE 的方向只能是 BUY/SELL，实际 side=" + side, symbol);
        }
        if (type == MarketDataType.QUOTE && !QUOTE_SIDES.contains(side)) {
            return reject(RejectReason.CONTRADICTORY_RECORD,
                    "QUOTE 的方向只能是 BID/ASK，实际 side=" + side, symbol);
        }
        if (!instrumentRegistry.isKnown(symbol)) {
            return reject(RejectReason.UNKNOWN_SYMBOL, "未知标的 symbol=" + symbol, symbol);
        }

        Instant eventTime = parseEventTime(raw.eventTime(), symbol);
        if (eventTime == null) {
            return reject(RejectReason.INVALID_TIMESTAMP,
                    "时间戳无法解析或非法 eventTime=" + raw.eventTime(), symbol);
        }
        if (eventTime.isBefore(MIN_VALID_TIME)) {
            return reject(RejectReason.INVALID_TIMESTAMP,
                    "时间戳过早（早于 " + MIN_VALID_TIME + "）eventTime=" + eventTime, symbol);
        }
        Instant now = Instant.now(clock);
        if (eventTime.isAfter(now.plusMillis(FUTURE_TOLERANCE_MILLIS))) {
            return reject(RejectReason.INVALID_TIMESTAMP,
                    "时间戳领先当前时间超过 " + FUTURE_TOLERANCE_MILLIS
                            + "ms，eventTime=" + eventTime + "，now=" + now, symbol);
        }

        BigDecimal price = parsePositiveDecimal(raw.price(), "price", symbol);
        if (price == null) {
            return reject(RejectReason.INVALID_NUMBER,
                    "价格缺失/不可解析/非正 price=" + raw.price(), symbol);
        }
        BigDecimal size = parsePositiveDecimal(raw.size(), "size", symbol);
        if (size == null) {
            return reject(RejectReason.INVALID_NUMBER,
                    "数量缺失/不可解析/非正 size=" + raw.size(), symbol);
        }

        NormalizedMarketData normalized = new NormalizedMarketData(
                raw.sourceId().trim(), raw.sequence(), type, symbol,
                eventTime, price, size, side);
        log.debug("归一化通过: {}", normalized);
        return IngestionOutcome.accepted(normalized,
                com.github.xingjianluo.marketdataingestionservicejava.model.AcceptedFlag.ACCEPTED);
    }

    private static Instant parseEventTime(String text, String symbol) {
        if (isBlank(text)) {
            return null;
        }
        try {
            return Instant.parse(text.trim());
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static BigDecimal parsePositiveDecimal(String text, String field, String symbol) {
        if (isBlank(text)) {
            return null;
        }
        final BigDecimal value;
        try {
            value = new BigDecimal(text.trim());
        } catch (NumberFormatException e) {
            return null;
        }
        return value.compareTo(BigDecimal.ZERO) > 0 ? value : null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static IngestionOutcome reject(RejectReason reason, String message, String symbol) {
        log.debug("归一化拒绝 reason={} 依据={}", reason, message);
        return IngestionOutcome.rejected(reason, message, symbol);
    }
}
