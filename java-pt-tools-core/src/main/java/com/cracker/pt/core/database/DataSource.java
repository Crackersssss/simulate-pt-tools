package com.cracker.pt.core.database;

import com.cracker.pt.core.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;

@Getter
public final class DataSource {

    private final String dataSourceName;

    private final HikariDataSource hikariDataSource;

    public DataSource(final String dataSourceName, final HikariConfig config) {
        this.dataSourceName = dataSourceName;
        this.hikariDataSource = new HikariDataSource(config);
    }

    public DataSource(final Config config) {
        this.dataSourceName = config.getDataSourceName();
        this.hikariDataSource = new HikariDataSource(config.getHikariConfig());
    }
}
