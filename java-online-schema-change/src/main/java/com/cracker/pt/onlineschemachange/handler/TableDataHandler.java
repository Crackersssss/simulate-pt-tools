package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TableDataHandler {

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
