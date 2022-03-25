package com.cracker.pt.tablechecksum.data;

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
}
