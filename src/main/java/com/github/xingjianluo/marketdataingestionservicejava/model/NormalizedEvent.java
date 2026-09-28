package com.github.xingjianluo.marketdataingestionservicejava.model;

/**
 * 归一化后的内部统一模型，同时作为日志记录与恢复回放的载体。
 *
 * <p>身份由 {@code (sourceId, offset)}（源位点）与 {@code (symbol, sequence)}
 * （标的序号）两层键共同确定；{@link #contentSignature()} 用于区分
 * “同身份的重复投递”与“同身份但内容矛盾”。
 */
public record NormalizedEvent(
        String sourceId,
        long offset,
        String symbol,
        long sequence,
        EventType type,
        long eventTime,
        long receivedTime,
        Double price,
        Double size,
        Double bidPrice,
        Double bidSize,
        Double askPrice,
        Double askSize
) {

    /**
     * 内容签名：同源同序号但下列业务字段不同即视为相互矛盾，
     * 重复投递（幂等）要求签名完全一致。
     */
    public String contentSignature() {
        return type + "|" + eventTime
                + "|" + nz(price) + "|" + nz(size)
                + "|" + nz(bidPrice) + "|" + nz(bidSize)
                + "|" + nz(askPrice) + "|" + nz(askSize);
    }

    private static String nz(Double v) {
        return v == null ? "-" : v.toString();
    }
}
