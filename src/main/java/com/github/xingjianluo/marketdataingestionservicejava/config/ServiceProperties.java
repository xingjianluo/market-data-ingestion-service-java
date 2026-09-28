package com.github.xingjianluo.marketdataingestionservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地服务可调参数：
 * <ul>
 *   <li>lateWindowMillis：可接受迟到窗口；超出窗口的迟到行情被拒绝而非混入结果；</li>
 *   <li>stateFile：聚合/位点快照的本地 JSON 文件；</li>
 *   <li>demoEnabled：启动时是否自动投递确定性演示脚本。</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "marketdata")
public class ServiceProperties {
    private long lateWindowMillis = 2_000L;
    private String stateFile = "data/marketdata-state.json";
    private boolean demoEnabled = false;

    public long getLateWindowMillis() {
        return lateWindowMillis;
    }

    public void setLateWindowMillis(long lateWindowMillis) {
        this.lateWindowMillis = lateWindowMillis;
    }

    public String getStateFile() {
        return stateFile;
    }

    public void setStateFile(String stateFile) {
        this.stateFile = stateFile;
    }

    public boolean isDemoEnabled() {
        return demoEnabled;
    }

    public void setDemoEnabled(boolean demoEnabled) {
        this.demoEnabled = demoEnabled;
    }
}
