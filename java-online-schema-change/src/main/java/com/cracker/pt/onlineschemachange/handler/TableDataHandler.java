package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class TableDataHandler {

    public static String generateCopyStatement(final List<String> columns, final String tableName, final String newTableName) {
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        String copyStatement = "insert into " + newTableName + "( " + columnNames + ")";
        String selectSQL = "select ";
        selectSQL = selectSQL + columnNames;
        selectSQL = selectSQL + " from " + tableName;
        copyStatement = copyStatement + " (" + selectSQL + ");";
        return copyStatement;
    }

    public static void copyData(final DataSource dataSource, final String sql) {
        try {
            HikariDataSource hikariDataSource = dataSource.getHikariDataSource();
            Connection connection = hikariDataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
