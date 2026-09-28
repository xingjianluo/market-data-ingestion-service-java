package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.aggregation.AggregationService;
import com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionStatus;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import com.github.xingjianluo.marketdataingestionservicejava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.xingjianluo.marketdataingestionservicejava.tests.TestEvents.tradeFrom;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发写入测试。
 *
 * <p>并发与时序假设（见 docs）：
 * <ul>
 *   <li>每个源的位点由该源按序产生；系统保证“并发写入同一状态”的串行化，
 *       但不把“源自己乱发位点”当作可修复的数据。</li>
 *   <li>标的序号允许在缓冲宽度（16）内乱序，系统缓冲并连续排空。</li>
 * </ul>
 */
class AggregationConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(AggregationConcurrencyTest.class);

    /**
     * 每个 writer 是一个独立源（位点严格连续），多源多标的并发写入，
     * 最终聚合必须与串行参考逐字段一致，且无重复计价。
     */
    @Test
    void concurrentWritersProduceDeterministicAggregates() throws Exception {
        long baseTime = 10_000_000L;
        int writers = 8;
        int perWriter = 200;
        // 每个 writer = 一个独立源 + 一个独立标的，序号空间互不串扰
        List<String> symbols = List.of(
                "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD",
                "ADA-USD", "DOGE-USD", "DOT-USD", "MATIC-USD");

        AggregationService concurrent = new AggregationService(new MarketDataProperties());
        AggregationService serial = new AggregationService(new MarketDataProperties());

        List<NormalizedEvent> allEvents = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        AtomicLong accepted = new AtomicLong();

        for (int w = 0; w < writers; w++) {
            final int writer = w;
            final String symbol = symbols.get(w);
            pool.submit(() -> {
                try {
                    start.await();
                    // 该源内部严格按 offset/sequence 递增生成与提交
                    for (int i = 0; i < perWriter; i++) {
                        NormalizedEvent e = tradeFrom(
                                "src-" + writer, i + 1L, symbol, i + 1L,
                                baseTime + i, 100.0 + (i % 10), 1.0);
                        allEvents.add(e);
                        var r = concurrent.process(e);
                        if (r.outcome().status() == IngestionStatus.ACCEPTED) {
                            accepted.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "并发写入应在超时前完成");
        pool.shutdown();

        allEvents.forEach(serial::process);

        for (String symbol : symbols) {
            var c = concurrent.snapshot(symbol);
            var ser = serial.snapshot(symbol);
            log.info("[{}] 并发快照 accepted={}, volume={}, lastSeq={}",
                    symbol, c.acceptedCount(), c.totalVolume(), c.lastSequence());
            assertEquals(ser, c, symbol + " 的并发结果必须与串行参考一致");
            assertEquals(perWriter, c.acceptedCount(), symbol + " 不得丢事件或重复计价");
            assertEquals(perWriter, c.lastSequence());
            assertEquals(0L, c.rejectedCount(), symbol + " 按序数据不应有拒绝");
            assertEquals(0L, c.duplicateCount());
        }
        assertEquals((long) writers * perWriter, accepted.get());
    }

    /**
     * 同一标的、缓冲宽度内的乱序并发：所有事件都被接受（立即或缓冲后排空），
     * 最终每条恰好计价一次。每个事件来自独立源，源位点层无冲突。
     */
    @Test
    void concurrentOutOfOrderOnSameSymbolEventuallyDrainsAll() throws Exception {
        long baseTime = 20_000_000L;
        // n 必须 <= 缓冲宽度+1，否则过宽缺口按设计拒绝
        int n = 16;
        AggregationService svc = new AggregationService(new MarketDataProperties());
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong accepted = new AtomicLong();

        List<NormalizedEvent> shuffled = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            shuffled.add(tradeFrom("src-" + i, 1L, "BTC-USD", i,
                    baseTime + i, 100.0 + (i % 5), 1.0));
        }
        Collections.shuffle(shuffled);

        CountDownLatch doneLatch = new CountDownLatch(n);
        for (NormalizedEvent e : shuffled) {
            pool.submit(() -> {
                try {
                    start.await();
                    log.info("并发输入 symbol={} seq={}", e.symbol(), e.sequence());
                    if (svc.process(e).outcome().status() == IngestionStatus.ACCEPTED) {
                        accepted.incrementAndGet();
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        var snap = svc.snapshot("BTC-USD");
        log.info("同标的并发乱序最终快照={}", snap);
        assertEquals(n, accepted.get(), "窗口内事件都应被接受（立即或缓冲后排空）");
        assertEquals(n, snap.acceptedCount(), "每条恰好计价一次");
        assertEquals(n, snap.lastSequence(), "缓冲应已全部排空");
        assertEquals((double) n, snap.totalVolume(), 1e-9);
        assertEquals(0L, snap.rejectedCount());
    }

    /**
     * 超过缓冲宽度的乱序即使并发到达也必须明确拒绝（SEQUENCE_GAP），
     * 而不是被无限缓冲或静默丢弃。
     */
    @Test
    void concurrentFarAheadSequenceIsRejectedAsGap() {
        long baseTime = 30_000_000L;
        AggregationService svc = new AggregationService(new MarketDataProperties());
        var first = svc.process(tradeFrom("s1", 1L, "BTC-USD", 1, baseTime, 100.0, 1.0));
        var farAhead = svc.process(tradeFrom("s2", 1L, "BTC-USD", 50, baseTime + 1, 100.0, 1.0));
        assertEquals(IngestionStatus.ACCEPTED, first.outcome().status());
        assertEquals(IngestionStatus.REJECTED, farAhead.outcome().status());
        assertEquals(RejectReason.SEQUENCE_GAP, farAhead.outcome().reason());
        log.info("超宽缺口判定: {}", farAhead.outcome());
    }
}
