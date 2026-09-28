package com.github.xingjianluo.marketdataingestionservicejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * REST 接口黑盒测试：拒绝/重复返回 HTTP 200，结论在响应体 status 字段。
 * 通过初始化器为每次上下文分配独占快照文件，彻底避免与其他测试类/历史运行互相恢复；
 * 每方法重建上下文，保证方法间状态干净。
 */
@SpringBootTest
@AutoConfigureMockMvc
@ContextConfiguration(initializers = WebApiIntegrationTests.IsolatedStateFile.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WebApiIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(WebApiIntegrationTests.class);

    /** 在 Spring 上下文创建前注入该上下文独占的状态文件。 */
    static class IsolatedStateFile
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            try {
                var dir = Files.createTempDirectory("mdis-web-");
                String file = dir.resolve("web-state-" + UUID.randomUUID() + ".json").toString();
                context.getEnvironment().getPropertySources()
                        .addFirst(new org.springframework.core.env.MapPropertySource(
                                "isolated-state", java.util.Map.of(
                                "marketdata.state-file", file,
                                "marketdata.demo-enabled", "false",
                                "marketdata.late-window-millis", "100000")));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void ingestAcceptedThenRejectedThenDuplicate() throws Exception {
        String trade1 = """
                {"sourceId":"WEB-A","sequence":1,"type":"TRADE","symbol":"600519.SH",
                 "eventTime":"2026-09-28T05:00:00Z","price":"100.00","size":"10","side":"BUY"}
                """;
        log.info("HTTP 接入首条: {}", trade1);
        mockMvc.perform(post("/api/marketdata/ingest")
                        .contentType(MediaType.APPLICATION_JSON).content(trade1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        String unknown = """
                {"sourceId":"WEB-A","sequence":2,"type":"TRADE","symbol":"NOPE.X",
                 "eventTime":"2026-09-28T05:00:01Z","price":"100","size":"1","side":"BUY"}
                """;
        log.info("HTTP 接入未知标的: {}", unknown);
        mockMvc.perform(post("/api/marketdata/ingest")
                        .contentType(MediaType.APPLICATION_JSON).content(unknown))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.rejectReason").value("UNKNOWN_SYMBOL"));

        log.info("HTTP 重复接入首条");
        mockMvc.perform(post("/api/marketdata/ingest")
                        .contentType(MediaType.APPLICATION_JSON).content(trade1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DUPLICATE"))
                .andExpect(jsonPath("$.duplicateReason").value("SAME_SEQUENCE_SAME_PAYLOAD"));

        mockMvc.perform(get("/api/marketdata/aggregates/600519.SH"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.acceptedCount").value(1))
                .andExpect(jsonPath("$.duplicateCount").value(1))
                .andExpect(jsonPath("$.rejectedCount").value(0));

        mockMvc.perform(get("/api/marketdata/sources/WEB-A"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lastSequence").value(1));
    }
}
