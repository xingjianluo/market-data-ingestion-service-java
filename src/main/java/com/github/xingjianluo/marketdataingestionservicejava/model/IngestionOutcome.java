package com.github.xingjianluo.marketdataingestionservicejava.model;

/** 单条行情经接入管线后的对外结论。 */
public record IngestionOutcome(
        IngestionStatus status,
        RejectReason reason,
        String sourceId,
        Long offset,
        String symbol,
        Long sequence,
        Long eventTime,
        String detail
) {
}
