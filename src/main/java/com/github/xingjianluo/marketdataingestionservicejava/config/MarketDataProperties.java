package com.github.xingjianluo.marketdataingestionservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * 行情服务配置（前缀 market-data）。
 *
 * @param maxOutOfOrderMillis 可接受的最大迟到时间窗（事件时间，毫秒）；
 *                            早于 watermark 超过该窗口的数据被标记为 LATE_OUTSIDE_WINDOW
 * @param journalPath         仅追加事件日志的本地文件路径（JSON Lines），用于重启重放
 * @param symbols             已知标的代码白名单
 */
@ConfigurationProperties(prefix = "market-data")
public class MarketDataProperties {

    /** 默认允许 5 秒的乱序迟到窗口。 */
    private long maxOutOfOrderMillis = 5_000L;

    /** 默认写到系统临时目录，避免污染工作区。 */
    private String journalPath = System.getProperty("java.io.tmpdir")
            + "/market-data-journal.jsonl";

    private List<String> symbols = List.of(
                "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD",
                "ADA-USD", "DOGE-USD", "DOT-USD", "MATIC-USD", "LTC-USD");

    public long getMaxOutOfOrderMillis() {
        return maxOutOfOrderMillis;
    }

    public void setMaxOutOfOrderMillis(long maxOutOfOrderMillis) {
        this.maxOutOfOrderMillis = maxOutOfOrderMillis;
    }

    public String getJournalPath() {
        return journalPath;
    }

    public void setJournalPath(String journalPath) {
        this.journalPath = journalPath;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
    }
}
