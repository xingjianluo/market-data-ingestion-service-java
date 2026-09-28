package com.github.xingjianluo.marketdataingestionservicejava.model;

/**
 * 单条报文接入判定结果。
 * <ul>
 *   <li>status=ACCEPTED：flag/normalized 非空，flag 区分正常接受与窗口内迟到接受；</li>
 *   <li>status=REJECTED：rejectReason/message 非空，message 用中文给出判定依据；</li>
 *   <li>status=DUPLICATE：duplicateReason/message 非空，区分完全重复与位点已提交。</li>
 * </ul>
 */
public record IngestionOutcome(
        IngestionStatus status,
        AcceptedFlag flag,
        RejectReason rejectReason,
        DuplicateReason duplicateReason,
        String message,
        NormalizedMarketData normalized,
        String symbol
) {
    public static IngestionOutcome accepted(NormalizedMarketData n, AcceptedFlag flag) {
        return new IngestionOutcome(IngestionStatus.ACCEPTED, flag, null, null,
                (flag == AcceptedFlag.ACCEPTED_LATE_IN_WINDOW
                        ? "接受：迟到数据但仍在可接受时间窗口内"
                        : "接受：通过全部校验并按序聚合"),
                n, n.symbol());
    }

    public static IngestionOutcome rejected(RejectReason reason, String message, String symbol) {
        return new IngestionOutcome(IngestionStatus.REJECTED, null, reason, null,
                "拒绝：" + message, null, symbol);
    }

    public static IngestionOutcome duplicate(DuplicateReason reason, String message, String symbol) {
        return new IngestionOutcome(IngestionStatus.DUPLICATE, null, null, reason,
                "重复：" + message, null, symbol);
    }
}
