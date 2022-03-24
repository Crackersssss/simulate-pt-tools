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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryColumns {

    private static final String SEPARATOR = "\\.";

    private static final String SEMICOLON = ";";

    private static final String STRING_FORMAT = "{0}:{1}";

    private static final String SQL = "select * from ";

    public static Map<String, List<String>> queryData(final List<DataSource> dataSources, final List<Table> tables) {
        Map<String, List<String>> results = new HashMap<>();
        dataSources.forEach(each -> {
            tables.forEach(element -> {
                try {
                    Connection connection = each.getHikariDataSource().getConnection();
                    Statement statement = connection.createStatement();
                    java.lang.String sql = SQL + element.getTableName() + SEMICOLON;
                    ResultSet resultSet = statement.executeQuery(sql);
                    results.put(each.getDataSourceName() + SEPARATOR + element.getTableName(), processingData(resultSet));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });
        return results;
    }

    public static List<String> processingData(final ResultSet resultSet) {
        List<String> result = new ArrayList<>();
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
                    result.add(String.valueOf(resultBuilder));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
