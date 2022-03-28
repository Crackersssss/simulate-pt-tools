package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.result.Result;
import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class TableCreateHandler extends Result {

    @Getter
    private static String newTableName;

    public static void createTable(final DataSource dataSource, final String tableName) throws SQLException {
        String createStatement = generateCreateTableStatement(dataSource, tableName);
        try {
            HikariDataSource hikariDataSource = dataSource.getHikariDataSource();
            Connection connection = hikariDataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(createStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String generateCreateTableStatement(final DataSource dataSource, final String tableName) throws SQLException {
        String oldCreateTableStatement = getOldCreateTableStatement(showOldCreateTable(dataSource, tableName));
        String substring = oldCreateTableStatement.substring(oldCreateTableStatement.indexOf('`'), oldCreateTableStatement.indexOf('`', oldCreateTableStatement.indexOf('`') + 1) + 1);
        newTableName = "`" + tableName + "_pt_new`";
        return oldCreateTableStatement.replace(substring, newTableName);
    }

    public static ResultSet showOldCreateTable(final DataSource dataSource, final String tableName) throws SQLException {
        HikariDataSource hikariDataSource = dataSource.getHikariDataSource();
        Connection connection = hikariDataSource.getConnection();
        Statement statement = connection.createStatement();
        String sql = "show create table " + tableName + ";";
        return statement.executeQuery(sql);
    }

    public static String getOldCreateTableStatement(final ResultSet resultSet) throws SQLException {
        String result = null;
        if (resultSet != null) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                StringBuilder resultBuilder = getResult(resultSet, metaData, columnCount);
                result = String.valueOf(resultBuilder).split("Create Table:")[1];
            }
            resultSet.close();
        }
        return result;
    }
}
