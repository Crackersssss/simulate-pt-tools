package com.cracker.pt.tablechecksum.data;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class DataSource {

    private static List<HikariConfig> getConfig() {
        ArrayList<HikariConfig> configs = new ArrayList<>();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false");
        config.setUsername("root");
        config.setPassword("root$123");
        configs.add(config);
        HikariConfig config2 = new HikariConfig();
        config2.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/pt_db2?useUnicode=true&characterEncoding=utf8&useSSL=false");
        config2.setUsername("root");
        config2.setPassword("root$123");
        configs.add(config2);
        return configs;
    }

    private static List<HikariDataSource> getDataSource() {
        List<HikariDataSource> dataSources = new ArrayList<>();
        getConfig().forEach(each -> dataSources.add(new HikariDataSource(each)));
        return dataSources;
    }

    public static List<ResultSet> select() {
        List<ResultSet> resultSets = new ArrayList<>();
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                String sql = "select * from pt_table;";
                resultSets.add(statement.executeQuery(sql));
                if (!each.isClosed()) {
                    each.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        return resultSets;
    }

    public static List<String> printResultSet(final List<ResultSet> resultSets) {
        List<String> result = new ArrayList<>();
        resultSets.forEach(each -> {
            if (each != null) {
                try {
                    ResultSetMetaData metaData = each.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (each.next()) {
                        StringBuilder resultBuilder = new StringBuilder();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object values = each.getObject(i);
                            resultBuilder.append(MessageFormat.format("{0}:{1}", columnName, values));
                        }
                        result.add(String.valueOf(resultBuilder));
                    }
                    each.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        return result;
    }

    public static void main(String[] args){
        List<String> results = DataSource.printResultSet(DataSource.select());
        results.forEach(System.out::println);
    }
}
