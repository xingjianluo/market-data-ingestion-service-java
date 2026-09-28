package com.github.xingjianluo.marketdataingestionservicejava.normalize;

import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * 已知标的注册表：模拟证券主数据。
 * 仅注册表内的标的可被接受，未知标的以 UNKNOWN_SYMBOL 拒绝而非静默丢弃。
 */
@Component
public class InstrumentRegistry {

    /** 本地已知标的代码集合。 */
    private static final Set<String> KNOWN_SYMBOLS = Set.of(
            "600519.SH",
            "000001.SZ",
            "AAPL.US",
            "BTC-USD"
    );

    public boolean isKnown(String symbol) {
        return symbol != null && KNOWN_SYMBOLS.contains(symbol);
    }
}
