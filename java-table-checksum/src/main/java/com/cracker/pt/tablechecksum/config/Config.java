package com.cracker.pt.tablechecksum.config;

import com.zaxxer.hikari.HikariConfig;
import lombok.Getter;

import java.util.Properties;

@Getter
public final class Config {

    private final String dataSourceName;

    private final HikariConfig hikariConfig;

    public Config(final String dataSourceName, final Properties connectionProperties) {
        this.dataSourceName = dataSourceName;
        this.hikariConfig = new HikariConfig(connectionProperties);
    }
}
