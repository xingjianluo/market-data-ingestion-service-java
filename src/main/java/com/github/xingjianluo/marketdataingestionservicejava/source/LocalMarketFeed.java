package com.github.xingjianluo.marketdataingestionservicejava.source;

import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketEvent;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 本地模拟行情源。
 *
 * <p>每个场景使用独立的源标识（{@code local-feed-<场景名>}），因此场景之间互不占用
 * 位点，可重复演示；同一场景内部仍然共用一个源，从而能真实触发重复投递、同位点篡改、
 * 位点回退与位点跳空。覆盖：正常到达、乱序到达（缓冲+排空）、重复投递（同内容）、
 * 矛盾投递（同序号不同内容）、同位点篡改、序号/位点缺口、位点回退、窗口外迟到、
 * 穿价成交、字段缺失与未知标的。
 */
@Component
public class LocalMarketFeed {

    private static final List<String> SCENARIOS = List.of(
            "happy", "out-of-order", "duplicate", "contradiction", "offset-tamper",
            "gap", "rollback", "late", "crossed-trade", "invalid", "mixed");

    public List<String> scenarios() {
        return SCENARIOS;
    }

    /** 返回指定场景的报文序列（保持场景设计的“到达顺序”）。 */
    public List<RawMarketEvent> scenario(String name) {
        long t = System.currentTimeMillis();
        return switch (name) {
            case "happy" -> happy(t, src(name), sym(name));
            case "out-of-order" -> outOfOrder(t, src(name), sym(name));
            case "duplicate" -> duplicate(t, src(name), sym(name));
            case "contradiction" -> contradiction(t, src(name), sym(name));
            case "offset-tamper" -> offsetTamper(t, src(name), sym(name));
            case "gap" -> gap(t, src(name), sym(name));
            case "rollback" -> rollback(t, src(name), sym(name));
            case "late" -> late(t, src(name), sym(name));
            case "crossed-trade" -> crossedTrade(t, src(name), sym(name));
            case "invalid" -> invalid(t);
            case "mixed" -> mixed(t, src(name));
            default -> throw new IllegalArgumentException("未知场景: " + name
                    + ", 可用: " + SCENARIOS);
        };
    }

    private static String src(String scenario) {
        return "local-feed-" + scenario;
    }

    /** 每个单标的场景使用独立标的，避免跨场景共用序号空间；均在白名单内。 */
    private static String sym(String scenario) {
        return switch (scenario) {
            case "happy" -> "BTC-USD";
            case "out-of-order" -> "ETH-USD";
            case "duplicate" -> "SOL-USD";
            case "contradiction" -> "XRP-USD";
            case "offset-tamper" -> "ADA-USD";
            case "gap" -> "DOGE-USD";
            case "rollback" -> "DOT-USD";
            case "late" -> "MATIC-USD";
            case "crossed-trade" -> "LTC-USD";
            default -> "BTC-USD";
        };
    }

    /** 正常顺序：先报价后成交，序号与位点都连续。 */
    public List<RawMarketEvent> happy(long t, String s, String sym) {
        return List.of(
                quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0),
                trade(s, 2, sym, 2, t + 10, 100.2, 2.0),
                quote(s, 3, sym, 3, t + 20, 100.1, 1.5, 100.6, 1.5),
                trade(s, 4, sym, 4, t + 30, 100.3, 1.0)
        );
    }

    /** 乱序到达：事件时间顺序不变，到达顺序为 1,3,2,4。seq3 在窗口内先缓冲后排空。 */
    public List<RawMarketEvent> outOfOrder(long t, String s, String sym) {
        RawMarketEvent q1 = quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0);
        RawMarketEvent tr2 = trade(s, 2, sym, 2, t + 10, 100.2, 2.0);
        RawMarketEvent q3 = quote(s, 3, sym, 3, t + 20, 100.1, 1.5, 100.6, 1.5);
        RawMarketEvent tr4 = trade(s, 4, sym, 4, t + 30, 100.3, 1.0);
        return List.of(q1, q3, tr2, tr4);
    }

    /** 重复投递：offset=2 的成交原样再投一次（同源同位点同内容）。 */
    public List<RawMarketEvent> duplicate(long t, String s, String sym) {
        RawMarketEvent q1 = quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0);
        RawMarketEvent tr2 = trade(s, 2, sym, 2, t + 10, 100.2, 2.0);
        RawMarketEvent tr2Again = trade(s, 2, sym, 2, t + 10, 100.2, 2.0);
        return List.of(q1, tr2, tr2Again);
    }

    /**
     * 矛盾投递（标的序号层）：seq=2 已接受，随后一条新位点（offset=3）
     * 却再次声称 seq=2 且价格被篡改，以 SEQUENCE_CONFLICT 拒绝。
     */
    public List<RawMarketEvent> contradiction(long t, String s, String sym) {
        RawMarketEvent q1 = quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0);
        RawMarketEvent tr2 = trade(s, 2, sym, 2, t + 10, 100.2, 2.0);
        RawMarketEvent tr2Mutated = trade(s, 3, sym, 2, t + 10, 999.99, 2.0);
        return List.of(q1, tr2, tr2Mutated);
    }

    /**
     * 同位点篡改（源位点层）：offset=2 的成交已接受，相同 offset 再次到达但价格不同，
     * 以 OFFSET_ROLLBACK 拒绝（区别于幂等重复 DUPLICATE_OFFSET）。
     */
    public List<RawMarketEvent> offsetTamper(long t, String s, String sym) {
        RawMarketEvent q1 = quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0);
        RawMarketEvent tr2 = trade(s, 2, sym, 2, t + 10, 100.2, 2.0);
        RawMarketEvent tr2Tampered = trade(s, 2, sym, 3, t + 20, 999.99, 2.0);
        return List.of(q1, tr2, tr2Tampered);
    }

    /**
     * 序号缺口：seq=2 之后直接到达 seq=30（超出缓冲宽度 16），缺口必须可见并拒绝。
     */
    public List<RawMarketEvent> gap(long t, String s, String sym) {
        return List.of(
                quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0),
                trade(s, 2, sym, 2, t + 10, 100.2, 2.0),
                trade(s, 30, sym, 30, t + 30, 100.3, 1.0)
        );
    }

    /**
     * 源位点回退：位点先推进到较高水位，再收到一个明显落后、且从未见过的位点
     * （落后超过缓冲宽度），以 OFFSET_ROLLBACK 拒绝。
     */
    public List<RawMarketEvent> rollback(long t, String s, String sym) {
        RawMarketEvent q1 = quote(s, 20, sym, 1, t, 100.0, 1.0, 100.5, 1.0);
        RawMarketEvent tr2 = trade(s, 21, sym, 2, t + 10, 100.2, 2.0);
        RawMarketEvent rolledBack = trade(s, 1, sym, 3, t + 20, 100.3, 1.0);
        return List.of(q1, tr2, rolledBack);
    }

    /**
     * 窗口外迟到：先把事件时间推到 t+31s，再到达一条 t+1s 的成交，
     * 晚于水位超过默认 5s 窗口，以 LATE_OUTSIDE_WINDOW 拒绝。
     */
    public List<RawMarketEvent> late(long t, String s, String sym) {
        return List.of(
                quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0),
                trade(s, 2, sym, 2, t + 10, 100.2, 2.0),
                quote(s, 3, sym, 3, t + 31_000, 100.4, 1.0, 100.9, 1.0),
                trade(s, 4, sym, 4, t + 1_000, 100.5, 1.0)
        );
    }

    /** 穿价成交：成交价落在最优报价之外。 */
    public List<RawMarketEvent> crossedTrade(long t, String s, String sym) {
        return List.of(
                quote(s, 1, sym, 1, t, 100.0, 1.0, 100.5, 1.0),
                trade(s, 2, sym, 2, t + 10, 101.0, 2.0)
        );
    }

    /** 各种字段问题：缺字段、非法时间戳、未知标的、买价高于卖价、未知类型。 */
    public List<RawMarketEvent> invalid(long t) {
        String s = "local-feed-invalid";
        return List.of(
                new RawMarketEvent(null, 1L, "BTC-USD", 1L, "QUOTE", t,
                        null, null, 100.0, 1.0, 100.5, 1.0),
                new RawMarketEvent(s, 2L, "BTC-USD", 2L, "TRADE", -1L,
                        100.2, 2.0, null, null, null, null),
                new RawMarketEvent(s, 3L, "FOO-USD", 3L, "TRADE", t + 10L,
                        0.5, 100.0, null, null, null, null),
                quote(s, 4, "BTC-USD", 4L, t + 20, 101.0, 1.0, 100.0, 1.0),
                new RawMarketEvent(s, 5L, "BTC-USD", 5L, "WAT", t + 30L,
                        null, null, null, null, null, null)
        );
    }

    /**
     * 混合场景：两个标的交错到达，含一次合法重复与一次位点跳空，
     * 用于端到端验证计数与结论一致性。
     */
    public List<RawMarketEvent> mixed(long t, String s) {
        List<RawMarketEvent> ordered = new java.util.ArrayList<>();
        ordered.add(quote(s, 1, "BTC-USD", 1, t, 100.0, 1.0, 100.5, 1.0));
        ordered.add(quote(s, 2, "ETH-USD", 1, t, 3000.0, 1.0, 3000.5, 1.0));
        ordered.add(trade(s, 3, "BTC-USD", 2, t + 10, 100.2, 2.0));
        ordered.add(trade(s, 4, "ETH-USD", 2, t + 10, 3000.2, 1.0));
        // 合法重复投递：offset=3 原样再来
        ordered.add(trade(s, 3, "BTC-USD", 2, t + 10, 100.2, 2.0));
        // 位点跳空：offset=30（远超缓冲宽度）
        ordered.add(trade(s, 30, "BTC-USD", 3, t + 20, 100.3, 1.0));
        return List.copyOf(ordered);
    }

    public static RawMarketEvent quote(String source, long offset, String symbol, long sequence,
                                       long eventTime,
                                       double bidPrice, double bidSize,
                                       double askPrice, double askSize) {
        return new RawMarketEvent(source, offset, symbol, sequence, "QUOTE", eventTime,
                null, null, bidPrice, bidSize, askPrice, askSize);
    }

    public static RawMarketEvent trade(String source, long offset, String symbol, long sequence,
                                       long eventTime, double price, double size) {
        return new RawMarketEvent(source, offset, symbol, sequence, "TRADE", eventTime,
                price, size, null, null, null, null);
    }
}
