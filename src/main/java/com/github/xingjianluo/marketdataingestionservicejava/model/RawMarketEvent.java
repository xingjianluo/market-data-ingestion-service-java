package com.github.xingjianluo.marketdataingestionservicejava.model;

/**
 * 本地模拟行情源的原始报文（JSON 接入协议）。
 *
 * @param sourceId  行情源标识
 * @param offset    该源内单调递增的位点（重放/去重用）
 * @param symbol    标的代码
 * @param sequence  标的内单调递增序号（聚合顺序/去重用）
 * @param type      "TRADE" 或 "QUOTE"
 * @param eventTime 事件时间（epoch 毫秒）
 * @param price     成交价（TRADE）或中间价（可选）
 * @param size      成交量（TRADE）
 * @param bidPrice  买一价（QUOTE）
 * @param bidSize   买一量（QUOTE）
 * @param askPrice  卖一价（QUOTE）
 * @param askSize   卖一量（QUOTE）
 */
public record RawMarketEvent(
        String sourceId,
        Long offset,
        String symbol,
        Long sequence,
        String type,
        Long eventTime,
        Double price,
        Double size,
        Double bidPrice,
        Double bidSize,
        Double askPrice,
        Double askSize
) {
}
