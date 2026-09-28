package com.github.xingjianluo.marketdataingestionservicejava.tests;

import com.github.xingjianluo.marketdataingestionservicejava.model.RawMarketEvent;
import com.github.xingjianluo.marketdataingestionservicejava.source.LocalMarketFeed;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 本地模拟源：每个场景都应产生非空、字段齐备的报文，且未知场景显式报错。 */
class LocalMarketFeedTest {

    private static final Logger log = LoggerFactory.getLogger(LocalMarketFeedTest.class);

    @Test
    void everyScenarioProducesEventsAndUnknownScenarioFails() {
        LocalMarketFeed feed = new LocalMarketFeed();
        for (String name : feed.scenarios()) {
            List<RawMarketEvent> events = feed.scenario(name);
            log.info("场景 {} 产生 {} 条报文", name, events.size());
            assertNotNull(events);
            assertTrue(events.size() >= 2, name + " 至少应包含 2 条报文");
            // invalid 场景故意制造缺字段报文，不做字段完备性断言
            if (!"invalid".equals(name)) {
                for (RawMarketEvent e : events) {
                    assertNotNull(e.sourceId());
                    assertNotNull(e.offset());
                    assertNotNull(e.symbol());
                    assertNotNull(e.sequence());
                    assertNotNull(e.type());
                    assertNotNull(e.eventTime());
                }
            }
        }
        assertThrows(IllegalArgumentException.class, () -> feed.scenario("nope"));
    }
}
