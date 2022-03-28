package com.cracker.pt.tablechecksum.query;

import com.cracker.pt.core.result.Result;
import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.core.database.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class QueryColumns extends Result {

    private static final String SEPARATOR = "\\.";

    private static final String SEMICOLON = ";";

    private static final String SQL = "select * from ";

    private QueryColumns() {
        throw new IllegalStateException("Utility class");
    }

    public static Map<String, List<String>> queryData(final List<DataSource> dataSources, final List<Table> tables) {
        Map<String, List<String>> results = new HashMap<>();
        dataSources.forEach(each -> tables.forEach(element -> {
            try {
                Connection connection = each.getHikariDataSource().getConnection();
                Statement statement = connection.createStatement();
                String sql = SQL + element.getTableName() + SEMICOLON;
                ResultSet resultSet = statement.executeQuery(sql);
                results.put(each.getDataSourceName() + SEPARATOR + element.getTableName(), processingData(resultSet));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }));
        return results;
    }

    public static List<String> processingData(final ResultSet resultSet) {
        List<String> result = new ArrayList<>();
        if (resultSet != null) {
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    StringBuilder resultBuilder = getResult(resultSet, metaData, columnCount);
                    result.add(String.valueOf(resultBuilder));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
