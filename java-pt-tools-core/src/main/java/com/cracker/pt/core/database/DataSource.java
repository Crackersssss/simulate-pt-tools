package com.cracker.pt.core.database;

import com.cracker.pt.core.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;

@Getter
public final class DataSource {

    private final String dataSourceName;

    private final String databaseName;

    private final HikariDataSource hikariDataSource;

    public DataSource(final String dataSourceName, final HikariConfig config, final String databaseName) {
        this.dataSourceName = dataSourceName;
        this.databaseName = databaseName;
        this.hikariDataSource = new HikariDataSource(config);
    }

    public DataSource(final Config config, String databaseName) {
        this.dataSourceName = config.getDataSourceName();
        this.hikariDataSource = new HikariDataSource(config.getHikariConfig());
        this.databaseName = databaseName;
    }
}
