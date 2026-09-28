package com.github.xingjianluo.marketdataingestionservicejava.replay;

import com.github.xingjianluo.marketdataingestionservicejava.model.SourceStatus;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 每源序号跟踪器：识别重复投递、同序号矛盾报文与序号缺口。
 *
 * <p>确定规则（每源一把锁，check 与占用原子完成）：
 * <ul>
 *   <li>{@code seq == lastContiguous+1}：NEW，原子占用并推进连续序号水位；</li>
 *   <li>{@code seq > lastContiguous+1}：GAP，拒绝并记录缺口（不静默丢弃、不跳号）；</li>
 *   <li>{@code seq <= lastContiguous}：报文已被处理过——载荷指纹相同判为完全重复，
 *       不同判为同序号相互矛盾，二者结论可区分。</li>
 * </ul>
 * 重复投递（含 at-least-once 重发）永不二次累加。
 */
@Component
public class SourceSequenceTracker {

    /** 单源的序号状态。所有变更都在 this 监视器内完成。 */
    static final class SourceState {
        long lastContiguous;
        long maxSeen;
        /** 曾“见到过”的序号：包含已接受与因缺口被挂起的，用于区分真正缺失的序号。 */
        final TreeSet<Long> seen = new TreeSet<>();
        final TreeSet<Long> accepted = new TreeSet<>();
        final Map<Long, String> fingerprints = new java.util.HashMap<>();
        long acceptedCount;
        long duplicateCount;
        long rejectedCount;
        long lastCommittedOffset = -1L;
    }

    private final ConcurrentHashMap<String, SourceState> sources = new ConcurrentHashMap<>();

    /** 序号检查结论。 */
    public enum SequenceDecision {
        NEW,
        DUPLICATE_SAME,
        DUPLICATE_CONTRADICTORY,
        GAP
    }

    /** 判定结论携带解释信息。 */
    public record Decision(SequenceDecision type, String message) {
    }

    private SourceState stateOf(String sourceId) {
        return sources.computeIfAbsent(sourceId, k -> new SourceState());
    }

    /**
     * 原子地检查并（当为下一条连续序号时）占用该序号。
     *
     * @param payloadFingerprint 归一化载荷指纹，用于区分完全重复与同序号矛盾
     */
    public Decision acceptIfNew(String sourceId, long sequence, String payloadFingerprint) {
        SourceState st = stateOf(sourceId);
        synchronized (st) {
            st.maxSeen = Math.max(st.maxSeen, sequence);
            st.seen.add(sequence);
            if (sequence == st.lastContiguous + 1) {
                st.accepted.add(sequence);
                st.fingerprints.put(sequence, payloadFingerprint);
                st.lastContiguous = sequence;
                st.acceptedCount++;
                return new Decision(SequenceDecision.NEW,
                        "序号连续：source=" + sourceId + " seq=" + sequence
                                + "，水位推进至 " + st.lastContiguous);
            }
            if (sequence > st.lastContiguous + 1) {
                List<Long> missing = missingWithin(st, sequence);
                st.rejectedCount++;
                return new Decision(SequenceDecision.GAP,
                        "序号缺口：source=" + sourceId + " seq=" + sequence
                                + "，当前连续水位=" + st.lastContiguous
                                + "，缺失序号=" + missing);
            }
            String existing = st.fingerprints.get(sequence);
            st.duplicateCount++;
            if (existing != null && existing.equals(payloadFingerprint)) {
                return new Decision(SequenceDecision.DUPLICATE_SAME,
                        "同序号同载荷重复投递：source=" + sourceId + " seq=" + sequence);
            }
            return new Decision(SequenceDecision.DUPLICATE_CONTRADICTORY,
                    "同序号载荷相互矛盾：source=" + sourceId + " seq=" + sequence
                            + "，已接受指纹=" + existing + "，本次指纹=" + payloadFingerprint);
        }
    }

    private static List<Long> missingWithin(SourceState st, long untilExclusive) {
        List<Long> missing = new ArrayList<>();
        for (long s = st.lastContiguous + 1; s < untilExclusive; s++) {
            if (!st.seen.contains(s)) {
                missing.add(s);
            }
        }
        return missing;
    }

    /** 位点推进（由接入/重放流程在处理完成后调用）。 */
    public void commitOffset(String sourceId, long offset) {
        SourceState st = stateOf(sourceId);
        synchronized (st) {
            if (offset > st.lastCommittedOffset) {
                st.lastCommittedOffset = offset;
            }
        }
    }

    public long committedOffset(String sourceId) {
        SourceState st = sources.get(sourceId);
        if (st == null) {
            return -1L;
        }
        synchronized (st) {
            return st.lastCommittedOffset;
        }
    }

    public SourceStatus status(String sourceId) {
        SourceState st = sources.get(sourceId);
        if (st == null) {
            return null;
        }
        synchronized (st) {
            List<Long> missing = missingWithin(st, Math.max(st.maxSeen, st.lastContiguous) + 1);
            return new SourceStatus(sourceId, st.lastContiguous, st.acceptedCount,
                    st.duplicateCount, st.rejectedCount, !missing.isEmpty(), missing,
                    st.lastCommittedOffset < 0 ? null : st.lastCommittedOffset);
        }
    }

    public List<SourceStatus> allStatus() {
        List<SourceStatus> result = new ArrayList<>();
        for (String sourceId : sources.keySet()) {
            SourceStatus s = status(sourceId);
            if (s != null) {
                result.add(s);
            }
        }
        return result;
    }

    /** 导出可持久化快照。 */
    public Map<String, ReplayStateStore.SourceSnapshot> exportSnapshot() {
        Map<String, ReplayStateStore.SourceSnapshot> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, SourceState> e : sources.entrySet()) {
            SourceState st = e.getValue();
            synchronized (st) {
                out.put(e.getKey(), new ReplayStateStore.SourceSnapshot(
                        st.lastContiguous,
                        List.copyOf(st.accepted),
                        Map.copyOf(st.fingerprints),
                        st.acceptedCount, st.duplicateCount, st.rejectedCount,
                        st.lastCommittedOffset));
            }
        }
        return out;
    }

    /** 从快照恢复（重启后调用）。 */
    public void importSnapshot(Map<String, ReplayStateStore.SourceSnapshot> snapshot) {
        sources.clear();
        snapshot.forEach((sourceId, s) -> {
            SourceState st = new SourceState();
            st.lastContiguous = s.lastContiguous();
            st.maxSeen = s.lastContiguous();
            st.seen.addAll(s.acceptedSequences());
            st.accepted.addAll(s.acceptedSequences());
            st.fingerprints.putAll(s.fingerprints());
            st.acceptedCount = s.acceptedCount();
            st.duplicateCount = s.duplicateCount();
            st.rejectedCount = s.rejectedCount();
            st.lastCommittedOffset = s.lastCommittedOffset();
            sources.put(sourceId, st);
        });
    }
}
