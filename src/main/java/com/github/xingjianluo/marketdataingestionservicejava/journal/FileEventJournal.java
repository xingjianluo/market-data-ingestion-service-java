package com.github.xingjianluo.marketdataingestionservicejava.journal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties;
import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于本地 JSON Lines 文件的仅追加事件日志。
 *
 * <p>每条已接受事件序列化为一行 JSON 并立即 flush；进程重启后通过
 * {@link #replayAll()} 按写入顺序读回，重建聚合状态与位点/序号水位。
 * 文件内不加“已提交位点”等额外元数据：恢复完全等于把历史事件重放一遍，
 * 因此同一批数据即使重复投递，状态也不会重复累加。
 */
@Component
public class FileEventJournal implements EventJournal {

    private static final Logger log = LoggerFactory.getLogger(FileEventJournal.class);

    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object writeLock = new Object();

    public FileEventJournal(MarketDataProperties properties) {
        this.file = Path.of(properties.getJournalPath());
    }

    @PostConstruct
    public void init() throws IOException {
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        if (Files.notExists(file)) {
            Files.createFile(file);
        }
        log.info("事件日志就绪: {}", file.toAbsolutePath());
    }

    public Path path() {
        return file;
    }

    @Override
    public void append(NormalizedEvent event) {
        synchronized (writeLock) {
            try (var out = Files.newBufferedWriter(file,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                out.write(mapper.writeValueAsString(event));
                out.newLine();
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException("写入事件日志失败: " + file, e);
            }
        }
    }

    @Override
    public List<NormalizedEvent> replayAll() {
        List<NormalizedEvent> events = new ArrayList<>();
        if (Files.notExists(file)) {
            return events;
        }
        try {
            for (String line : Files.readAllLines(file)) {
                if (!line.isBlank()) {
                    events.add(mapper.readValue(line, NormalizedEvent.class));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取事件日志失败: " + file, e);
        }
        log.info("重放事件日志: {} 条, file={}", events.size(), file);
        return events;
    }
}
