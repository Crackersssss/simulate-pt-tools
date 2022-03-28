package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TableAlterHandler {

    public static void alterTableStruct(final DataSource dataSource, final String alterStatement) {
        try {
            HikariDataSource hikariDataSource = dataSource.getHikariDataSource();
            Connection connection = hikariDataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(alterStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
