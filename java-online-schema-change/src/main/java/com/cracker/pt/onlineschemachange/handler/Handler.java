package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class Handler {

    protected static final String END = ";";

    protected static final String SPACE = " ";

    private final HikariDataSource dataSource;

    protected Connection connection;

    protected Statement statement;

    protected Handler(DataSource dataSource) throws SQLException {
        this.dataSource = dataSource.getHikariDataSource();
        init();
    }

    protected void init() throws SQLException {
        this.connection = dataSource.getConnection();
        this.statement = connection.createStatement();
    }

    protected void close() throws SQLException {
        statement.close();
        connection.close();
    }
}
