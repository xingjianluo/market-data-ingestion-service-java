package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.replay.SourceSequenceTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 序号跟踪器单测：缺口、完全重复、同序号矛盾、多源独立计数。
 */
class SourceSequenceTrackerTests {

    private static final Logger log = LoggerFactory.getLogger(SourceSequenceTrackerTests.class);

    private SourceSequenceTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new SourceSequenceTracker();
    }

    private SourceSequenceTracker.Decision check(String source, long seq, String fp) {
        var d = tracker.acceptIfNew(source, seq, fp);
        log.info("source={} seq={} fp={} => {} | 依据={}", source, seq, fp, d.type(), d.message());
        return d;
    }

    @Test
    @DisplayName("连续序号被接受")
    void contiguousAccepted() {
        assertEquals(SourceSequenceTracker.SequenceDecision.NEW, check("A", 1, "fp1").type());
        assertEquals(SourceSequenceTracker.SequenceDecision.NEW, check("A", 2, "fp2").type());
        assertEquals(2L, tracker.status("A").lastSequence());
        assertFalse(tracker.status("A").gapPending());
    }

    @Test
    @DisplayName("跳号触发 GAP，缺口补齐后状态清晰")
    void gapDetectedThenFilled() {
        assertEquals(SourceSequenceTracker.SequenceDecision.NEW, check("A", 1, "fp1").type());
        var gap = check("A", 3, "fp3");
        assertEquals(SourceSequenceTracker.SequenceDecision.GAP, gap.type());
        assertTrue(tracker.status("A").gapPending());
        assertEquals(java.util.List.of(2L), tracker.status("A").missingSequences());

        // seq=2 补齐
        assertEquals(SourceSequenceTracker.SequenceDecision.NEW, check("A", 2, "fp2").type());
        assertEquals(2L, tracker.status("A").lastSequence());
        // 之前 GAP 拒绝的 seq=3 未占用，现在补齐水位后可被重新接受（源侧重传）
        assertEquals(SourceSequenceTracker.SequenceDecision.NEW, check("A", 3, "fp3").type());
        assertEquals(3L, tracker.status("A").lastSequence());
        assertFalse(tracker.status("A").gapPending());
    }

    @Test
    @DisplayName("完全重复与同序号矛盾给出可区分结论")
    void duplicateSameVsContradiction() {
        check("A", 1, "fp-original");
        var same = check("A", 1, "fp-original");
        var contradiction = check("A", 1, "fp-tampered");
        assertEquals(SourceSequenceTracker.SequenceDecision.DUPLICATE_SAME, same.type());
        assertEquals(SourceSequenceTracker.SequenceDecision.DUPLICATE_CONTRADICTORY,
                contradiction.type());

        var st = tracker.status("A");
        assertEquals(1, st.acceptedCount());
        assertEquals(2, st.duplicateCount());
    }

    @Test
    @DisplayName("不同行情源序号空间相互独立")
    void sourcesAreIndependent() {
        check("A", 1, "a1");
        var b1 = check("B", 1, "b1");
        var aGap = check("A", 3, "a3");
        var b2 = check("B", 2, "b2");
        assertEquals(SourceSequenceTracker.SequenceDecision.NEW, b1.type());
        assertEquals(SourceSequenceTracker.SequenceDecision.GAP, aGap.type());
        assertEquals(SourceSequenceTracker.SequenceDecision.NEW, b2.type());
    }

    @Test
    @DisplayName("位点只单调推进")
    void offsetMonotonic() {
        tracker.commitOffset("A", 5);
        tracker.commitOffset("A", 3);
        assertEquals(5L, tracker.committedOffset("A"));
        tracker.commitOffset("A", 8);
        assertEquals(8L, tracker.committedOffset("A"));
    }
}
