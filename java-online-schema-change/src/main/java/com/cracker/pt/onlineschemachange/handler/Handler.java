package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Operating handler.
 */
public abstract class Handler {

    @Getter
    private final HikariDataSource dataSource;

    @Getter
    private final String databaseName;

    @Getter
    @Setter
    private Connection connection;

    protected Handler(final DataSource dataSource) {
        this.databaseName = dataSource.getDatabaseName();
        this.dataSource = dataSource.getHikariDataSource();
    }

    protected void init() throws SQLException {
        this.connection = dataSource.getConnection();
    }

    protected Statement getStatement() throws SQLException {
        return connection.createStatement();
    }

    protected void begin() throws SQLException {
        connection.setAutoCommit(false);
    }

    protected void commit() throws SQLException {
        connection.commit();
    }

    protected void close() throws SQLException {
        connection.close();
    }

    protected void rollback() throws SQLException {
        connection.rollback();
    }
}
