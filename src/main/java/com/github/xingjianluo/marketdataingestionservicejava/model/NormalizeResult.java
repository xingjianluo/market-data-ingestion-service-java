package com.github.xingjianluo.marketdataingestionservicejava.model;

/** 归一化结果：成功携带事件，失败携带可区分的拒绝原因。 */
public record NormalizeResult(
        boolean valid,
        NormalizedEvent event,
        RejectReason reason
) {

    public static NormalizeResult accepted(NormalizedEvent event) {
        return new NormalizeResult(true, event, null);
    }

    public static NormalizeResult rejected(RejectReason reason) {
        return new NormalizeResult(false, null, reason);
    }
}
