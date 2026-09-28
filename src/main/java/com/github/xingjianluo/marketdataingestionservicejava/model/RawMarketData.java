package com.github.xingjianluo.marketdataingestionservicejava.model;

/**
 * 本地模拟行情源投递的原始报文。字符串/数值均为外部格式，由归一化层校验转换。
 *
 * @param sourceId 行情源标识（序号按源独立编号）
 * @param sequence 报文在该源内的单调序号
 * @param type     TRADE / QUOTE
 * @param symbol   标的代码
 * @param eventTime 事件时间（ISO-8601 字符串，由源时钟产生）
 * @param price    成交价（TRADE）或报价价格（QUOTE）
 * @param size     成交量（TRADE）或报价量（QUOTE）
 * @param side     TRADE: BUY/SELL；QUOTE: BID/ASK
 */
public record RawMarketData(
        String sourceId,
        Long sequence,
        String type,
        String symbol,
        String eventTime,
        String price,
        String size,
        String side
) {
}
