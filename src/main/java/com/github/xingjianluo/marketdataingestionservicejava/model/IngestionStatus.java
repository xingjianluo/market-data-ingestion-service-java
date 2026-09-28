package com.github.xingjianluo.marketdataingestionservicejava.model;

/** 单条行情对外的判定结论：已接受 / 已拒绝 / 已重复。 */
public enum IngestionStatus {
    ACCEPTED,
    REJECTED,
    DUPLICATE
}
