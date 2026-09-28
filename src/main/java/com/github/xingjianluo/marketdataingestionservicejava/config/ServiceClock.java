package com.github.xingjianluo.marketdataingestionservicejava.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** 统一时钟：生产使用系统 UTC 时钟，测试可替换为固定时钟。 */
@Configuration
public class ServiceClock {
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
