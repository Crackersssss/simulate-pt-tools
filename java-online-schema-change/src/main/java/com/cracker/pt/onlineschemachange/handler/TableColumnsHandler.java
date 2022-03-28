package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.core.result.Result;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableColumnsHandler extends Result {

    public static List<String> getAllColumns(final DataSource dataSource, final String tableName) {
        List<String> resultSets = new ArrayList<>();
        try {
            HikariDataSource hikariDataSource = dataSource.getHikariDataSource();
            Connection connection = hikariDataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SHOW COLUMNS FROM " + tableName + ";");
            while (resultSet.next()) {
                resultSets.add(String.valueOf(resultSet.getString("Field")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSets;
    }
}
