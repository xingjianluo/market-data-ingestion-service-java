package com.github.xingjianluo.marketdataingestionservicejava.replay;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestionservicejava.config.ServiceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 重放状态持久化：把“每标的聚合状态 + 每源序号/位点状态”写入本地 JSON 文件，
 * 启动时加载恢复。
 *
 * <p>保证：重启后从已提交位点重放同一批数据，不会重复累加（序号去重）、
 * 不会丢失（聚合快照完整恢复）。写入采用临时文件 + 原子 move，避免半截文件。
 */
@Component
public class ReplayStateStore {

    private static final Logger log = LoggerFactory.getLogger(ReplayStateStore.class);

    private final ObjectMapper objectMapper;
    private final ServiceProperties properties;

    public ReplayStateStore(ObjectMapper objectMapper, ServiceProperties properties) {
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    // ---------------- 持久化 DTO ----------------

    /** 完整快照。 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Snapshot(
            String savedAt,
            Map<String, SourceSnapshot> sources,
            Map<String, SymbolSnapshot> symbols
    ) {
    }

    /** 每源序号/位点状态。 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SourceSnapshot(
            long lastContiguous,
            List<Long> acceptedSequences,
            Map<Long, String> fingerprints,
            long acceptedCount,
            long duplicateCount,
            long rejectedCount,
            long lastCommittedOffset
    ) {
        public SourceSnapshot() {
            this(0, List.of(), Map.of(), 0, 0, 0, -1L);
        }
    }

    /** 每标的聚合状态。 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SymbolSnapshot(
            String symbol,
            long acceptedCount,
            long duplicateCount,
            long rejectedCount,
            long tradeCount,
            String totalVolume,
            String totalTurnover,
            String lastTradePrice,
            String highestPrice,
            String lowestPrice,
            String latestBid,
            String latestAsk,
            String lastEventTime,
            String lastTradeKeyTime,
            String lastTradeKeySource,
            long lastTradeKeySeq,
            String lastBidKeyTime,
            String lastBidKeySource,
            long lastBidKeySeq,
            String lastAskKeyTime,
            String lastAskKeySource,
            long lastAskKeySeq,
            Long lastSequence,
            String lastSourceId,
            Map<String, String> maxEventTimeBySource,
            List<String> appliedKeys
    ) {
        public SymbolSnapshot() {
            this(null, 0, 0, 0, 0, "0", "0", null, null, null, null, null,
                    null, null, null, 0, null, null, 0, null, null, 0,
                    null, null, Map.of(), List.of());
        }
    }

    /** 由运行态收集快照内容（由服务层调用，避免反向依赖聚合包）。 */
    public void save(Snapshot snapshot) throws IOException {
        Path target = Path.of(properties.getStateFile());
        Path parent = target.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), snapshot);
        try {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicFail) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
        log.info("状态快照已持久化: {}", target.toAbsolutePath());
    }

    public Optional<Snapshot> load() {
        Path target = Path.of(properties.getStateFile());
        if (!Files.exists(target)) {
            log.info("未发现状态快照 {}，按全新状态启动", target.toAbsolutePath());
            return Optional.empty();
        }
        try {
            Snapshot snapshot = objectMapper.readValue(target.toFile(), Snapshot.class);
            log.info("已加载状态快照: {}", target.toAbsolutePath());
            return Optional.of(snapshot);
        } catch (IOException e) {
            log.error("状态快照损坏，无法加载: {}", target.toAbsolutePath(), e);
            return Optional.empty();
        }
    }

    public void clear() throws IOException {
        Files.deleteIfExists(Path.of(properties.getStateFile()));
    }

    /** 便于聚合层把 BigDecimal 转字符串。 */
    public static String s(BigDecimal v) {
        return v == null ? null : v.toPlainString();
    }

    /** 便于聚合层把 Instant 转字符串。 */
    public static String s(Instant v) {
        return v == null ? null : v.toString();
    }

    /** 便于聚合层把字符串转 BigDecimal。 */
    public static BigDecimal bd(String v) {
        return v == null ? null : new BigDecimal(v);
    }

    /** 空快照。 */
    public static Snapshot emptySnapshot() {
        return new Snapshot(Instant.now().toString(),
                new LinkedHashMap<>(), new LinkedHashMap<>());
    }
}
