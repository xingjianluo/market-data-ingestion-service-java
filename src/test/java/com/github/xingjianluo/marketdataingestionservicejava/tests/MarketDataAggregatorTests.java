package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregate.MarketDataAggregator;
import com.github.xingjianluo.marketdataingestionservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.MarketDataType;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.SymbolAggregateView;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 聚合器单测：以乱序到达、重复投递、并发写入为主，验证结果与到达顺序无关、
 * 同序号不重复计价、窗口外迟到被明确拒绝。
 */
class MarketDataAggregatorTests {

    private static final Logger log = LoggerFactory.getLogger(MarketDataAggregatorTests.class);

    private static final Instant BASE = Instant.parse("2026-09-28T03:00:00Z");

    private MarketDataAggregator newAggregator(long windowMillis) {
        ServiceProperties props = new ServiceProperties();
        props.setLateWindowMillis(windowMillis);
        return new MarketDataAggregator(Clock.fixed(BASE, ZoneOffset.UTC), props);
    }

    private NormalizedMarketData trade(long seq, long offsetMs, String price, long size,
                                       String source) {
        return new NormalizedMarketData(source, seq, MarketDataType.TRADE, "600519.SH",
                BASE.plusMillis(offsetMs), new BigDecimal(price), new BigDecimal(size),
                seq % 2 == 0 ? "SELL" : "BUY");
    }

    private NormalizedMarketData quote(long seq, long offsetMs, String price, String side) {
        return new NormalizedMarketData("Q", seq, MarketDataType.QUOTE, "600519.SH",
                BASE.plusMillis(offsetMs), new BigDecimal(price), new BigDecimal(1), side);
    }

    private void logView(String label, SymbolAggregateView v) {
        log.info("[{}] symbol={} accepted={} dup={} rejected={} trades={} volume={} "
                        + "turnoverVwap={} high={} low={} last={} bid={} ask={}",
                label, v.symbol(), v.acceptedCount(), v.duplicateCount(), v.rejectedCount(),
                v.tradeCount(), v.totalVolume(), v.vwap(), v.highestPrice(), v.lowestPrice(),
                v.lastTradePrice(), v.latestBid(), v.latestAsk());
    }

    @Test
    @DisplayName("乱序到达：正序与乱序聚合结果完全一致")
    void outOfOrderGivesSameResultAsInOrder() {
        List<NormalizedMarketData> ordered = List.of(
                trade(1, 0, "100", 10, "A"),
                trade(2, 100, "101", 20, "A"),
                trade(3, 200, "99", 5, "A"));
        List<NormalizedMarketData> shuffled = new ArrayList<>(ordered);
        Collections.reverse(shuffled);

        MarketDataAggregator a1 = newAggregator(100_000);
        ordered.forEach(a1::apply);
        MarketDataAggregator a2 = newAggregator(100_000);
        shuffled.forEach(a2::apply);

        SymbolAggregateView v1 = a1.view("600519.SH").orElseThrow();
        SymbolAggregateView v2 = a2.view("600519.SH").orElseThrow();
        logView("正序", v1);
        logView("乱序", v2);

        assertEquals(v1.acceptedCount(), v2.acceptedCount());
        assertEquals(0, v1.totalVolume().compareTo(new BigDecimal("35")));
        assertEquals(0, v1.vwap().compareTo(v2.vwap()));
        assertEquals(v1.lastTradePrice(), v2.lastTradePrice());
        // 最新价按事件时间取 99，而非最后到达的价格
        assertEquals(0, v1.lastTradePrice().compareTo(new BigDecimal("99")));
        assertEquals(0, v1.highestPrice().compareTo(new BigDecimal("101")));
        assertEquals(0, v1.lowestPrice().compareTo(new BigDecimal("99")));
    }

    @Test
    @DisplayName("重复投递：同 (源,序号) 只计价一次，返回 DUPLICATE")
    void duplicateSequenceAppliedOnce() {
        MarketDataAggregator agg = newAggregator(100_000);
        var r1 = agg.apply(trade(1, 0, "100", 10, "A"));
        var r2 = agg.apply(trade(1, 0, "100", 10, "A"));
        log.info("首次={} 依据={}", r1.status(), r1.message());
        log.info("重复={} 依据={}", r2.status(), r2.message());
        assertEquals(MarketDataAggregator.ApplyStatus.APPLIED, r1.status());
        assertEquals(MarketDataAggregator.ApplyStatus.DUPLICATE, r2.status());

        SymbolAggregateView v = agg.view("600519.SH").orElseThrow();
        logView("重复后", v);
        assertEquals(1, v.acceptedCount());
        assertEquals(1, v.duplicateCount());
        assertEquals(0, v.totalVolume().compareTo(new BigDecimal("10")));
    }

    @Test
    @DisplayName("迟到窗口：窗口内接受并标记，窗口外拒绝且不混入")
    void lateWindowEnforced() {
        MarketDataAggregator agg = newAggregator(2_000);
        // 先到 t=10000 的主序行情
        assertEquals(MarketDataAggregator.ApplyStatus.APPLIED,
                agg.apply(trade(1, 10_000, "110", 10, "A")).status());
        // t=9000，落后 1000ms ≤ 2000ms：窗口内迟到
        var inWindow = agg.apply(trade(2, 9_000, "109", 10, "A"));
        log.info("窗口内迟到={} 依据={}", inWindow.status(), inWindow.message());
        assertEquals(MarketDataAggregator.ApplyStatus.APPLIED_LATE, inWindow.status());

        SymbolAggregateView before = agg.view("600519.SH").orElseThrow();
        logView("窗口内迟到后", before);
        assertEquals(2, before.acceptedCount());
        assertEquals(0, before.totalVolume().compareTo(new BigDecimal("20")));

        // t=5000，落后 5000ms > 2000ms：窗口外迟到，拒绝
        var outWindow = agg.apply(trade(3, 5_000, "101", 10, "A"));
        log.info("窗口外迟到={} 依据={}", outWindow.status(), outWindow.message());
        assertEquals(MarketDataAggregator.ApplyStatus.LATE_OUT_OF_WINDOW, outWindow.status());

        SymbolAggregateView after = agg.view("600519.SH").orElseThrow();
        logView("窗口外迟到后", after);
        assertEquals(2, after.acceptedCount(), "窗口外数据不得混入");
        assertEquals(1, after.rejectedCount());
        assertEquals(0, after.totalVolume().compareTo(new BigDecimal("20")));
        // 窗口外被拒绝的序号键未被占用：后续相同键以更新内容到达时应仍可处理（键已释放）
        // 这里验证计数，不验证再次到达
    }

    @Test
    @DisplayName("跨源不互判迟到：A 源晚事件不影响 B 源较早事件接受")
    void lateIsPerSourceNotPerSymbol() {
        MarketDataAggregator agg = newAggregator(2_000);
        // A 源事件时间很晚
        assertEquals(MarketDataAggregator.ApplyStatus.APPLIED,
                agg.apply(trade(1, 10_000, "110", 10, "A")).status());
        // B 源较早事件：相对 B 自己没有水位，必须正常接受，不能被 A 判成窗口外迟到
        var b = agg.apply(trade(1, 0, "100", 3, "B"));
        log.info("跨源较早事件={} 依据={}", b.status(), b.message());
        assertEquals(MarketDataAggregator.ApplyStatus.APPLIED, b.status());
        // A 源自己的窗口外迟到仍要拒绝
        var aLate = agg.apply(trade(2, 5_000, "108", 2, "A"));
        assertEquals(MarketDataAggregator.ApplyStatus.LATE_OUT_OF_WINDOW, aLate.status());
    }

    @Test
    @DisplayName("报价按事件时间取最新 BID/ASK，与到达顺序无关")
    void quoteLatestByEventTime() {
        MarketDataAggregator agg = newAggregator(100_000);
        // 晚到的较旧报价不应覆盖较新报价
        agg.apply(quote(2, 200, "100.40", "BID"));
        agg.apply(quote(1, 100, "100.10", "BID"));
        agg.apply(quote(4, 400, "100.60", "ASK"));
        agg.apply(quote(3, 300, "100.90", "ASK"));
        SymbolAggregateView v = agg.view("600519.SH").orElseThrow();
        logView("报价乱序", v);
        assertEquals(0, v.latestBid().compareTo(new BigDecimal("100.40")));
        assertEquals(0, v.latestAsk().compareTo(new BigDecimal("100.60")));
    }

    @RepeatedTest(20)
    @DisplayName("并发写入：多线程混合乱序提交，结果确定且无重复计价")
    void concurrentWritesAreDeterministic() throws InterruptedException {
        MarketDataAggregator agg = newAggregator(100_000);
        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int worker = t;
            pool.submit(() -> {
                try {
                    start.await();
                    // 每个工作线程负责一组互不重叠的序号，但事件时间乱序分布
                    for (int i = 0; i < perThread; i++) {
                        long seq = (long) worker * perThread + i + 1;
                        long eventOffset = (seq * 37L) % 4000;
                        agg.apply(trade(seq, eventOffset, "100." + (eventOffset % 50), 1, "A"));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        boolean finished = done.await(30, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(true, finished, "并发任务应在超时前完成");

        SymbolAggregateView v = agg.view("600519.SH").orElseThrow();
        logView("并发完成", v);
        assertEquals(threads * perThread, v.acceptedCount(), "每条唯一序号恰好计价一次");
        assertEquals(0, v.totalVolume().compareTo(new BigDecimal(threads * perThread)),
                "成交量必须等于唯一消息数");
        assertEquals(threads * perThread, v.tradeCount());
        assertNotNull(v.vwap());
    }

    @Test
    @DisplayName("并发重复投递：同一序号被多个线程同时提交，只成功一次")
    void concurrentDuplicateOnlyOneWins() throws Exception {
        MarketDataAggregator agg = newAggregator(100_000);
        NormalizedMarketData n = trade(1, 0, "100", 7, "A");
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<MarketDataAggregator.ApplyStatus> statuses =
                Collections.synchronizedList(new ArrayList<>());
        IntStream.range(0, threads).forEach(i -> pool.submit(() -> {
            try {
                start.await();
                statuses.add(agg.apply(n).status());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        long applied = statuses.stream().filter(s -> s != MarketDataAggregator.ApplyStatus.DUPLICATE
                && s != MarketDataAggregator.ApplyStatus.LATE_OUT_OF_WINDOW).count();
        SymbolAggregateView v = agg.view("600519.SH").orElseThrow();
        log.info("并发重复投递结果 statuses={}", statuses);
        logView("并发重复后", v);
        assertEquals(1, applied, "只能有一个线程实际计价");
        assertEquals(1, v.acceptedCount());
        assertEquals(0, v.totalVolume().compareTo(new BigDecimal("7")));
    }
}
