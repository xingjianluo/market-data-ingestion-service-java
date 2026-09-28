package com.github.xingjianluo.marketdataingestionservicejava.model;

/** 已重复的具体原因，用于对外解释“为什么判定为重复”。 */
public enum DuplicateReason {
    /** (源,序号) 已处理过且归一化载荷完全一致。 */
    SAME_SEQUENCE_SAME_PAYLOAD,
    /** 重放帧的位点已经提交过。 */
    ALREADY_COMMITTED_OFFSET
}
