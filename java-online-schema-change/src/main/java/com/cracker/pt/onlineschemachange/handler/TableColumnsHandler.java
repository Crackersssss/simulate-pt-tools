package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Table columns operating handler.
 */
public class TableColumnsHandler extends Handler {

    private static final String FIELD_COLUMN_NAME = "Field";

    public TableColumnsHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public List<String> getAllColumns(final String tableName) throws SQLException {
        List<String> resultSets = new ArrayList<>();
        String databaseName = getDatabaseName();
        String sql = String.format("SHOW COLUMNS FROM %s.%s;", databaseName,tableName);
        ResultSet resultSet = getStatement().executeQuery(sql);
        while (resultSet.next()) {
            resultSets.add(String.valueOf(resultSet.getString(FIELD_COLUMN_NAME)));
        }
        return resultSets;
    }

    @Override
    public void begin() throws SQLException {
        super.begin();
    }

    @Override
    public void commit() throws SQLException {
        super.commit();
    }

    @Override
    public void close() throws SQLException {
        super.close();
    }

    @Override
    public void rollback() throws SQLException {
        super.rollback();
    }
}
