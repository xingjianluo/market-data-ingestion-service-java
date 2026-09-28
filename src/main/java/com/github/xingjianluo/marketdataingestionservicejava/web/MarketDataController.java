package com.github.xingjianluo.marketdataingestionservicejava.web;

import com.github.xingjianluo.marketdataingestionservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketData;
import com.github.xingjianluo.marketdataingestionservicejava.model.SymbolAggregateView;
import com.github.xingjianluo.marketdataingestionservicejava.replay.ReplayCoordinator;
import com.github.xingjianluo.marketdataingestionservicejava.service.IngestionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 本地行情接入 REST 接口。
 *
 * <ul>
 *   <li>POST /api/marketdata/ingest：单条实时接入；</li>
 *   <li>POST /api/marketdata/replay?scenario=&amp;fromOffset=：本地脚本重放；</li>
 *   <li>GET  /api/marketdata/aggregates[/{symbol}]：聚合结果；</li>
 *   <li>GET  /api/marketdata/sources[/{sourceId}]：序号/位点状态。</li>
 * </ul>
 * 注意：拒绝/重复属于正常业务结论，仍返回 HTTP 200；具体结论看响应体 status。
 */
@RestController
@RequestMapping("/api/marketdata")
public class MarketDataController {

    private final IngestionService ingestionService;
    private final ReplayCoordinator replayCoordinator;

    public MarketDataController(IngestionService ingestionService,
                                ReplayCoordinator replayCoordinator) {
        this.ingestionService = ingestionService;
        this.replayCoordinator = replayCoordinator;
    }

    @PostMapping("/ingest")
    public IngestionOutcome ingest(@RequestBody RawMarketData raw) {
        return ingestionService.ingest(raw);
    }

    @PostMapping("/replay")
    public ReplayCoordinator.ReplaySession replay(@RequestParam String scenario,
                                                  @RequestParam(defaultValue = "0") long fromOffset) {
        return replayCoordinator.replay(scenario, fromOffset);
    }

    @GetMapping("/aggregates")
    public List<SymbolAggregateView> aggregates() {
        return ingestionService.allAggregates();
    }

    @GetMapping("/aggregates/{symbol}")
    public ResponseEntity<Object> aggregate(@PathVariable String symbol) {
        return ingestionService.aggregateOf(symbol)
                .<ResponseEntity<Object>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("message", "尚无该标的聚合结果", "symbol", symbol)));
    }

    @GetMapping("/sources")
    public List<Object> sources() {
        return List.copyOf(ingestionService.allSourceStatus());
    }

    @GetMapping("/sources/{sourceId}")
    public ResponseEntity<Object> source(@PathVariable String sourceId) {
        return ingestionService.sourceStatus(sourceId)
                .<ResponseEntity<Object>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("message", "尚无该源状态", "sourceId", sourceId)));
    }
}
