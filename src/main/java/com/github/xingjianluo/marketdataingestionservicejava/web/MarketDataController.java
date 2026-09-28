package com.github.xingjianluo.marketdataingestionservicejava.web;

import com.github.xingjianluo.marketdataingestionservicejava.aggregation.AggregationService;
import com.github.xingjianluo.marketdataingestionservicejava.ingest.MarketDataIngestor;
import com.github.xingjianluo.marketdataingestionservicejava.model.AggregateSnapshot;
import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketEvent;
import com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketFeed;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 本地行情接入与聚合查询的 HTTP 端点。
 *
 * <ul>
 *   <li>POST /api/events            单条行情接入</li>
 *   <li>POST /api/events/batch      批量行情接入（用于重放/重复投递验证）</li>
 *   <li>POST /api/simulate/{name}   投递本地模拟场景</li>
 *   <li>GET  /api/simulate/scenarios 列出可用场景</li>
 *   <li>GET  /api/aggregates        全部标的聚合快照</li>
 *   <li>GET  /api/aggregates/{symbol} 单标的聚合快照</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class MarketDataController {

    private final MarketDataIngestor ingestor;
    private final AggregationService aggregationService;
    private final LocalMarketFeed feed;

    public MarketDataController(MarketDataIngestor ingestor,
                                AggregationService aggregationService,
                                LocalMarketFeed feed) {
        this.ingestor = ingestor;
        this.aggregationService = aggregationService;
        this.feed = feed;
    }

    @PostMapping("/events")
    public IngestionOutcome postEvent(@RequestBody RawMarketEvent raw) {
        return ingestor.ingest(raw);
    }

    @PostMapping("/events/batch")
    public List<IngestionOutcome> postBatch(@RequestBody List<RawMarketEvent> batch) {
        return batch.stream().map(ingestor::ingest).toList();
    }

    @PostMapping("/simulate/{name}")
    public List<IngestionOutcome> runScenario(@PathVariable String name) {
        return feed.scenario(name).stream().map(ingestor::ingest).toList();
    }

    @GetMapping("/simulate/scenarios")
    public List<String> scenarios() {
        return feed.scenarios();
    }

    @GetMapping("/aggregates")
    public List<AggregateSnapshot> getSnapshots() {
        return aggregationService.snapshots();
    }

    @GetMapping("/aggregates/{symbol}")
    public AggregateSnapshot getSymbol(@PathVariable String symbol) {
        return aggregationService.snapshot(symbol);
    }
}
