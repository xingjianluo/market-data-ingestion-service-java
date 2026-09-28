package com.github.xingjianluo.marketdataingestionservicejava.model;

import java.util.List;

/** 单个行情源的序号/位点状态，用于观察缺口与回退。 */
public record SourceStatus(
        String sourceId,
        Long lastSequence,
        long acceptedCount,
        long duplicateCount,
        long rejectedCount,
        boolean gapPending,
        List<Long> missingSequences,
        Long lastCommittedOffset
) {
}
