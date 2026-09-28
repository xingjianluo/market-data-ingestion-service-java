package com.github.xingjianluo.marketdataingestionservicejava.model;

/**
 * 拒绝原因：用于按原因区分输入为何未被接受，结论必须可解释、可区分。
 * <ul>
 *   <li>{@link #MISSING_FIELD}：必填字段缺失或空白；</li>
 *   <li>{@link #INVALID_TIMESTAMP}：时间戳无法解析、为远古时间或超出时钟容忍范围；</li>
 *   <li>{@link #UNKNOWN_SYMBOL}：标的不在主数据注册表内；</li>
 *   <li>{@link #INVALID_NUMBER}：价格/数量无法解析或不大于 0；</li>
 *   <li>{@link #INVALID_PAYLOAD}：枚举型字段取值非法（未知行情类型/方向）；</li>
 *   <li>{@link #CONTRADICTORY_RECORD}：字段之间或与已接受同序号记录相互矛盾；</li>
 *   <li>{@link #SEQUENCE_GAP}：序号出现缺口（前序序号尚未到达）；</li>
 *   <li>{@link #OFFSET_ROLLBACK}：重放位点回退到已提交位点之前；</li>
 *   <li>{@link #LATE_OUT_OF_WINDOW}：事件时间过晚，超出可接受迟到窗口。</li>
 * </ul>
 */
public enum RejectReason {
    MISSING_FIELD,
    INVALID_TIMESTAMP,
    UNKNOWN_SYMBOL,
    INVALID_NUMBER,
    INVALID_PAYLOAD,
    CONTRADICTORY_RECORD,
    SEQUENCE_GAP,
    OFFSET_ROLLBACK,
    LATE_OUT_OF_WINDOW
}
