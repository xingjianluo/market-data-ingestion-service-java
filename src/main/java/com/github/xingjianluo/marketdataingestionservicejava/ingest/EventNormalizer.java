package com.github.xingjianluo.marketdataingestionservicejava.ingest;

import com.github.xingjianluo.marketdataingestionservicejava.instrument.InstrumentRegistry;
import com.github.xingjianluo.marketdataingestionservicejava.model.EventType;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizeResult;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 原始报文归一化与校验。
 *
 * <p>检查顺序固定且短路：任何一条输入只会得到一个拒绝原因，便于对外解释。
 * 顺序为：必填字段（源/标的/位点/序号/时间）→ 类型 → 时间戳合法性 →
 * 标的白名单 → 类型相关字段完备性 → 数值合法性 → 报价内在一致性。
 * 与“已有记录”的矛盾在聚合阶段（TRADE_PRICE_CONTRADICTS_QUOTE）判定。
 */
@Component
public class EventNormalizer {

    /** 允许的时钟偏移：事件时间超过接收时间该值即视为非法时间戳。 */
    static final long FUTURE_TOLERANCE_MILLIS = 60_000L;

    private static final Logger log = LoggerFactory.getLogger(EventNormalizer.class);

    private final InstrumentRegistry instruments;

    public EventNormalizer(InstrumentRegistry instruments) {
        this.instruments = instruments;
    }

    public NormalizeResult normalize(RawMarketEvent raw) {
        long receivedTime = System.currentTimeMillis();

        if (raw == null || isBlank(raw.sourceId())) {
            return reject(raw, RejectReason.MISSING_SOURCE_ID, "sourceId 缺失");
        }
        if (isBlank(raw.symbol())) {
            return reject(raw, RejectReason.MISSING_SYMBOL, "symbol 缺失");
        }
        if (raw.offset() == null) {
            return reject(raw, RejectReason.MISSING_OFFSET, "offset 缺失");
        }
        if (raw.sequence() == null) {
            return reject(raw, RejectReason.MISSING_SEQUENCE, "sequence 缺失");
        }
        if (raw.eventTime() == null) {
            return reject(raw, RejectReason.MISSING_EVENT_TIME, "eventTime 缺失");
        }

        EventType type;
        if (isBlank(raw.type())) {
            return reject(raw, RejectReason.UNSUPPORTED_TYPE, "type 缺失");
        }
        try {
            type = EventType.valueOf(raw.type().trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return reject(raw, RejectReason.UNSUPPORTED_TYPE,
                    "不支持的类型: " + raw.type());
        }

        long eventTime = raw.eventTime();
        if (eventTime <= 0L || eventTime > receivedTime + FUTURE_TOLERANCE_MILLIS) {
            return reject(raw, RejectReason.INVALID_TIMESTAMP,
                    "非法时间戳 eventTime=" + eventTime + ", receivedTime=" + receivedTime);
        }

        if (!instruments.isKnown(raw.symbol())) {
            return reject(raw, RejectReason.UNKNOWN_SYMBOL,
                    "未知标的: " + raw.symbol() + ", 白名单=" + instruments.knownSymbols());
        }

        if (type == EventType.TRADE) {
            NormalizeResult fieldCheck = checkTrade(raw);
            if (fieldCheck != null) {
                return fieldCheck;
            }
        } else {
            NormalizeResult fieldCheck = checkQuote(raw);
            if (fieldCheck != null) {
                return fieldCheck;
            }
        }

        NormalizedEvent event = new NormalizedEvent(
                raw.sourceId().trim(),
                raw.offset(),
                raw.symbol().trim(),
                raw.sequence(),
                type,
                eventTime,
                receivedTime,
                type == EventType.TRADE ? raw.price() : null,
                type == EventType.TRADE ? raw.size() : null,
                type == EventType.QUOTE ? raw.bidPrice() : null,
                type == EventType.QUOTE ? raw.bidSize() : null,
                type == EventType.QUOTE ? raw.askPrice() : null,
                type == EventType.QUOTE ? raw.askSize() : null
        );
        log.info("归一化接受: {} -> {}", raw, event);
        return NormalizeResult.accepted(event);
    }

    private NormalizeResult checkTrade(RawMarketEvent raw) {
        if (raw.price() == null || raw.size() == null) {
            return reject(raw, RejectReason.MISSING_TRADE_FIELDS,
                    "成交必须同时提供 price 与 size");
        }
        if (!isFinitePositive(raw.price())) {
            return reject(raw, RejectReason.INVALID_PRICE,
                    "成交价非法: " + raw.price());
        }
        if (!Double.isFinite(raw.size()) || raw.size() <= 0d) {
            return reject(raw, RejectReason.NEGATIVE_SIZE,
                    "成交量必须为有限正数: " + raw.size());
        }
        return null;
    }

    private NormalizeResult checkQuote(RawMarketEvent raw) {
        if (raw.bidPrice() == null || raw.askPrice() == null
                || raw.bidSize() == null || raw.askSize() == null) {
            return reject(raw, RejectReason.MISSING_QUOTE_FIELDS,
                    "报价必须同时提供 bidPrice/bidSize/askPrice/askSize");
        }
        if (!isFinitePositive(raw.bidPrice()) || !isFinitePositive(raw.askPrice())) {
            return reject(raw, RejectReason.INVALID_PRICE,
                    "报价价格非法: bid=" + raw.bidPrice() + ", ask=" + raw.askPrice());
        }
        if (!Double.isFinite(raw.bidSize()) || raw.bidSize() <= 0d
                || !Double.isFinite(raw.askSize()) || raw.askSize() <= 0d) {
            return reject(raw, RejectReason.NEGATIVE_SIZE,
                    "报价数量必须为有限正数: bidSize=" + raw.bidSize()
                            + ", askSize=" + raw.askSize());
        }
        if (raw.bidPrice() > raw.askPrice()) {
            return reject(raw, RejectReason.INCONSISTENT_QUOTE,
                    "买价高于卖价: bid=" + raw.bidPrice() + " > ask=" + raw.askPrice());
        }
        return null;
    }

    private static boolean isFinitePositive(Double v) {
        return v != null && Double.isFinite(v) && v > 0d;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private NormalizeResult reject(RawMarketEvent raw, RejectReason reason, String detail) {
        log.warn("归一化拒绝: input={}, reason={}, detail={}", raw, reason, detail);
        return NormalizeResult.rejected(reason);
    }
}
