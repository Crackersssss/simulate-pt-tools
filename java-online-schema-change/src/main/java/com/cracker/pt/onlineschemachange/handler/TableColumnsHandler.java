package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableColumnsHandler {

    public static List<String> getAllColumns(final DataSource dataSource) {
        List<String> resultSets = new ArrayList<>();
        try {
            HikariDataSource hikariDataSource = dataSource.getHikariDataSource();
            Connection connection = hikariDataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = connection.getMetaData().getTables(null, "pt_db", "%", new String[]{"TABLE"});
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                //System.out.println(tableName);

                if ("pt_ddl".equals(tableName)) {
                    ResultSet rs = connection.getMetaData().getColumns(null, "%", tableName.toUpperCase(), "%");

                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        resultSets.add(colName);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSets;
    }
}
