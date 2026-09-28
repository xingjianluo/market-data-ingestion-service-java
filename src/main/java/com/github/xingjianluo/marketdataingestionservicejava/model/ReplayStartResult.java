package com.github.xingjianluo.marketdataingestionservicejava.model;

/** 重放启动结论。 */
public record ReplayStartResult(
        String sourceId,
        ReplayVerdict verdict,
        Long fromOffset,
        Long committedOffset,
        String message
) {
}
