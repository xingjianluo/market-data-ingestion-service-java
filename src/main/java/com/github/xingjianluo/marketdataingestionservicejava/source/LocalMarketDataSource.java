package com.github.xingjianluo.marketdataingestionservicejava.source;

import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地模拟行情源：产出可重复生成的确定性脚本，覆盖
 * 正常顺序、乱序到达、重复投递（完全重复）、同序号矛盾、序号缺口、
 * 迟到（窗口内/窗口外）与各类非法报文。
 *
 * <p>每个脚本是一个 {@link Batch}：脚本下标即“投递位点 offset”，
 * 重放时从指定位点开始读取。
 */
@Component
public class LocalMarketDataSource {

    /** 固定基准时间，保证测试可重复；需要“相对当前”的场景由测试自行构造。 */
    public static final String BASE_TIME = "2026-09-28T01:00:00Z";

    private static final String SRC_A = "SIM-A";
    private static final String SRC_B = "SIM-B";
    private static final String M = "600519.SH";

    /** 带投递位点的一批脚本。 */
    public record Batch(String name, List<RawMarketData> records) {
        public List<RawMarketData> fromOffset(long offset) {
            if (offset < 0 || offset > records.size()) {
                return List.of();
            }
            return records.subList((int) offset, records.size());
        }

        public int size() {
            return records.size();
        }
    }

    /**
     * 取一个命名场景。
     * <ul>
     *   <li>ordered：正常基线；</li>
     *   <li>out-of-order-dup：乱序 + 完全重复 + 同序号矛盾；</li>
   *   <li>gap：序号缺口与缺口补齐；</li>
     *   <li>invalid：字段缺失/时间戳非法/未知标的等拒绝路径；</li>
     *   <li>late：窗口内迟到与窗口外迟到。</li>
     * </ul>
     */
    public Batch batch(String name) {
        return switch (name) {
            case "ordered" -> orderedBatch();
            case "out-of-order-dup" -> outOfOrderDupBatch();
            case "gap" -> gapBatch();
            case "invalid" -> invalidBatch();
            case "late" -> lateBatch();
            default -> orderedBatch();
        };
    }

    /** 兼容旧接口：直接取记录列表。 */
    public List<RawMarketData> scenario(String name) {
        return batch(name).records();
    }

    private Instant t(long millisAfterBase) {
        return Instant.parse(BASE_TIME).plusMillis(millisAfterBase);
    }

    private RawMarketData trade(String source, long seq, String symbol, Instant ts,
                                String price, String size, String side) {
        return new RawMarketData(source, seq, "TRADE", symbol, ts.toString(), price, size, side);
    }

    private RawMarketData quote(String source, long seq, String symbol, Instant ts,
                                String price, String size, String side) {
        return new RawMarketData(source, seq, "QUOTE", symbol, ts.toString(), price, size, side);
    }

    /** 正常顺序：连续序号、事件时间递增。 */
    private Batch orderedBatch() {
        List<RawMarketData> r = new ArrayList<>();
        r.add(trade(SRC_A, 1, M, t(0), "100.00", "10", "BUY"));
        r.add(trade(SRC_A, 2, M, t(100), "100.50", "20", "SELL"));
        r.add(quote(SRC_A, 3, M, t(150), "100.48", "15", "BID"));
        r.add(quote(SRC_A, 4, M, t(150), "100.52", "15", "ASK"));
        r.add(trade(SRC_A, 5, M, t(200), "101.00", "5", "BUY"));
        return new Batch("ordered", r);
    }

    /**
     * 乱序 + 完全重复 + 同序号矛盾：乱序本身不拒绝，由序号水位判定 GAP/后续补齐。
     * 注意：序号空间在源内对 TRADE/QUOTE 统一编号，seq=3 的原始报文是 QUOTE，
     * 其重传必须保持同一载荷；矛盾帧单独占用 seq=5。
     */
    private Batch outOfOrderDupBatch() {
        List<RawMarketData> r = new ArrayList<>();
        r.add(trade(SRC_A, 1, M, t(0), "100.00", "10", "BUY"));
        // seq=3 先到：缺口（缺 2），拒绝 SEQUENCE_GAP
        r.add(quote(SRC_A, 3, M, t(150), "100.48", "15", "BID"));
        // seq=2 补齐：接受
        r.add(trade(SRC_A, 2, M, t(100), "100.50", "20", "SELL"));
        // seq=3 重放（源侧重传，载荷一致）：接受（之前因 GAP 未被占用）
        r.add(quote(SRC_A, 3, M, t(150), "100.48", "15", "BID"));
        r.add(quote(SRC_A, 4, M, t(150), "100.52", "15", "ASK"));
        // seq=1 完全重复投递：DUPLICATE（同载荷）
        r.add(trade(SRC_A, 1, M, t(0), "100.00", "10", "BUY"));
        // seq=2 同序号不同载荷：CONTRADICTORY_RECORD
        r.add(trade(SRC_A, 2, M, t(100), "999.99", "20", "SELL"));
        r.add(trade(SRC_A, 5, M, t(200), "101.00", "5", "BUY"));
        return new Batch("out-of-order-dup", r);
    }

    /** 多源 + 缺口：不同源序号独立。 */
    private Batch gapBatch() {
        List<RawMarketData> r = new ArrayList<>();
        r.add(trade(SRC_A, 1, M, t(0), "100.00", "10", "BUY"));
        r.add(trade(SRC_B, 1, M, t(10), "100.01", "3", "SELL"));
        // A 源 seq=3：缺口（缺 2）
        r.add(trade(SRC_A, 3, M, t(300), "101.00", "5", "BUY"));
        // B 源 seq=2：正常（B 的水位独立）
        r.add(trade(SRC_B, 2, M, t(20), "100.02", "4", "BUY"));
        // A 源 seq=2 补齐
        r.add(trade(SRC_A, 2, M, t(100), "100.50", "20", "SELL"));
        // A 源 seq=3 重传：接受
        r.add(trade(SRC_A, 3, M, t(300), "101.00", "5", "BUY"));
        return new Batch("gap", r);
    }

    /** 各类非法/矛盾输入，覆盖全部拒绝原因（时序类除外）。 */
    private Batch invalidBatch() {
        List<RawMarketData> r = new ArrayList<>();
        r.add(new RawMarketData("  ", 1L, "TRADE", M, t(0).toString(), "100", "10", "BUY"));
        r.add(new RawMarketData(SRC_A, null, "TRADE", M, t(0).toString(), "100", "10", "BUY"));
        r.add(new RawMarketData(SRC_A, 2L, "FILL", M, t(0).toString(), "100", "10", "BUY"));
        r.add(trade(SRC_A, 3, "999999.XX", t(0), "100", "10", "BUY"));
        r.add(new RawMarketData(SRC_A, 4L, "TRADE", M, "not-a-time", "100", "10", "BUY"));
        r.add(new RawMarketData(SRC_A, 5L, "TRADE", M, "1990-01-01T00:00:00Z",
                "100", "10", "BUY"));
        r.add(trade(SRC_A, 6, M, t(0), "abc", "10", "BUY"));
        r.add(trade(SRC_A, 7, M, t(0), "0", "10", "BUY"));
        r.add(trade(SRC_A, 8, M, t(0), "100", "-5", "BUY"));
        r.add(new RawMarketData(SRC_A, 9L, "TRADE", M, t(0).toString(),
                "100", "10", "UP"));
        r.add(new RawMarketData(SRC_A, 10L, "QUOTE", M, t(0).toString(),
                "100", "10", "BUY"));
        return new Batch("invalid", r);
    }

    /** 迟到场景：需要配合固定/可控时钟使用，时间点由参数基准给出。 */
    private Batch lateBatch() {
        List<RawMarketData> r = new ArrayList<>();
        // 先到较晚事件，再到较早事件，模拟乱序迟到
        r.add(trade(SRC_A, 1, M, t(10_000), "101.00", "10", "BUY"));
        r.add(trade(SRC_A, 2, M, t(9_000), "100.50", "10", "SELL"));   // 窗口内迟到
        r.add(trade(SRC_A, 3, M, t(5_000), "100.10", "10", "SELL"));   // 窗口外迟到
        return new Batch("late", r);
    }

    /** 所有命名场景一览，便于演示逐个回放。 */
    public Map<String, Batch> allBatches() {
        Map<String, Batch> map = new LinkedHashMap<>();
        for (String name : List.of("ordered", "out-of-order-dup", "gap", "invalid", "late")) {
            map.put(name, batch(name));
        }
        return map;
    }
}
