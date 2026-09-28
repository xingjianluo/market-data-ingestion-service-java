package com.github.xingjianluo.marketdataingestionservicejava.instrument;

import com.github.xingjianluo.marketdataingestionservicejava.config.MarketDataProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 已知标的注册表。
 *
 * <p>白名单来自配置 {@code market-data.symbols}；未知标的在归一化阶段即以
 * {@code UNKNOWN_SYMBOL} 拒绝，而不是静默丢弃。
 */
@Component
public class InstrumentRegistry {

    private final Set<String> knownSymbols;

    public InstrumentRegistry(MarketDataProperties properties) {
        this.knownSymbols = new LinkedHashSet<>(properties.getSymbols());
    }

    public boolean isKnown(String symbol) {
        return symbol != null && knownSymbols.contains(symbol);
    }

    public Set<String> knownSymbols() {
        return Set.copyOf(knownSymbols);
    }
}
