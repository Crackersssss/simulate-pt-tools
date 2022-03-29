package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class Handler {

    protected static final String END = ";";

    protected static final String SPACE = " ";

    private final HikariDataSource dataSource;

    @Getter
    private Connection connection;

    @Getter
    private Statement statement;

    protected Handler(final DataSource dataSource) throws SQLException {
        this.dataSource = dataSource.getHikariDataSource();
        init();
    }

    protected void init() throws SQLException {
        this.connection = dataSource.getConnection();
        this.statement = connection.createStatement();
    }

    protected void begin() throws SQLException {
        connection.setAutoCommit(false);
    }

    protected void commit() throws SQLException {
        connection.commit();
    }

    protected void close() throws SQLException {
        statement.close();
        connection.close();
    }

    protected void rollback() throws SQLException {
        connection.rollback();
    }
}
