package com.github.xingjianluo.marketdataingestionservicejava.bootstrap;

import com.github.xingjianluo.marketdataingestionservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.SymbolAggregateView;
import com.github.xingjianluo.marketdataingestionservicejava.service.IngestionService;
import com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * 本地演示：启动后（marketdata.demo-enabled=true）顺序投递若干确定性脚本，
 * 在日志中逐条打印“输入 + 判定依据”，最后打印各标的聚合结果。
 */
@Component
public class LocalDemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LocalDemoRunner.class);

    private final IngestionService ingestionService;
    private final LocalMarketDataSource dataSource;
    private final ServiceProperties properties;

    public LocalDemoRunner(IngestionService ingestionService,
                           LocalMarketDataSource dataSource,
                           ServiceProperties properties) {
        this.ingestionService = ingestionService;
        this.dataSource = dataSource;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isDemoEnabled()) {
            return;
        }
        log.info("===== 本地行情接入演示开始 =====");
        for (String scenario : new String[]{"ordered", "out-of-order-dup", "gap", "invalid"}) {
            log.info("----- 场景: {} -----", scenario);
            // 每个场景使用独立的源命名空间（源ID作为前缀），避免跨场景序号撞号
            String sourcePrefix = "DEMO-" + scenario.toUpperCase().replace('-', '_') + "-";
            for (RawMarketData raw0 : dataSource.scenario(scenario)) {
                RawMarketData raw = withSourcePrefix(raw0, sourcePrefix);
                IngestionOutcome outcome = ingestionService.ingest(raw);
                log.info("[{}] 输入={} => 结论={} 原因={} 依据={}",
                        scenario, raw, outcome.status(),
                        outcome.rejectReason() != null ? outcome.rejectReason()
                                : outcome.duplicateReason() != null ? outcome.duplicateReason()
                                : outcome.flag(),
                        outcome.message());
            }
        }
        log.info("----- 最终聚合结果 -----");
        for (SymbolAggregateView view : ingestionService.allAggregates()) {
            log.info("聚合 {}: accepted={} duplicate={} rejected={} volume={} vwap={} last={} bid={} ask={}",
                    view.symbol(), view.acceptedCount(), view.duplicateCount(),
                    view.rejectedCount(), view.totalVolume(), view.vwap(),
                    view.lastTradePrice(), view.latestBid(), view.latestAsk());
        }
        log.info("===== 本地行情接入演示结束 =====");
    }

    /** 为报文的源标识加前缀，使不同脚本各自独立编号。 */
    private static RawMarketData withSourcePrefix(RawMarketData r, String prefix) {
        return new RawMarketData(prefix + r.sourceId(), r.sequence(), r.type(), r.symbol(),
                r.eventTime(), r.price(), r.size(), r.side());
    }
}
