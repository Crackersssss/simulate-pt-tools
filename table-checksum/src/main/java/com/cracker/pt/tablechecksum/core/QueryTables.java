package com.cracker.pt.tablechecksum.core;

import com.cracker.pt.tablechecksum.data.DataSource;
import com.cracker.pt.tablechecksum.data.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class QueryTables {

    private static final String STRING_FORMAT = "{0}:{1}";

    private static final String SQL = "show tables;";

    private QueryTables() {
        throw new IllegalStateException("Utility class");
    }

    public static List<Table> queryData(final List<DataSource> dataSources) {
        ArrayList<List<Table>> allTables = new ArrayList<>();
        dataSources.forEach(each -> {
            try {
                Connection connection = each.getHikariDataSource().getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(SQL);
                allTables.add(processingData(resultSet));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        ArrayList<String> tableNames = new ArrayList<>();
        allTables.forEach(each -> {
            StringBuilder tableName;
            tableName = each.stream().reduce(new StringBuilder(), (a, b) -> a.append(b.toString()), (a, b) -> null);
            tableNames.add(String.valueOf(tableName));
        });
        String tableName = tableNames.stream().reduce((a, b) -> String.valueOf(Compute.computeMD5(a).equals(Compute.computeMD5(b))))
                .<RuntimeException>orElseThrow(() -> {
                    throw new RuntimeException("Table name calculation error!");
                });
        if ("false".equals(tableName)) {
            throw new RuntimeException("The master and slave database table names or quantities are inconsistent!");
        } else if (String.valueOf(Boolean.TRUE).equals(tableName)) {
            return allTables.get(0);
        }
        return Collections.emptyList();
    }

    public static List<Table> processingData(final ResultSet resultSet) {
        List<Table> result = new ArrayList<>();
        if (resultSet != null) {
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    StringBuilder resultBuilder = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object values = resultSet.getObject(i);
                        resultBuilder.append(MessageFormat.format(STRING_FORMAT, columnName, values));
                    }
                    result.add(new Table(String.valueOf(resultBuilder)));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
