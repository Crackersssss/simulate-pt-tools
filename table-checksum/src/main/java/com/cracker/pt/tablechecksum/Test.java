package com.cracker.pt.tablechecksum;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import java.util.Optional;

public class Test {

    private static Map<String, HikariConfig> getConfig() {
        Map<String, HikariConfig> configs = new HashMap<>();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false");
        config.setUsername("root");
        config.setPassword("root$123");
        configs.put("resource_0", config);
        HikariConfig config2 = new HikariConfig();
        config2.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/pt_db2?useUnicode=true&characterEncoding=utf8&useSSL=false");
        config2.setUsername("root");
        config2.setPassword("root$123");
        configs.put("resource_1", config2);
        return configs;
    }

    private static Map<String, HikariDataSource> getDataSource(Map<String, HikariConfig> config) {
        Map<String, HikariDataSource> dataSources = new HashMap<>();
        config.forEach((k, v) -> dataSources.put(k, new HikariDataSource(v)));
        return dataSources;
    }

    private static Map<String, ResultSet> selectTables(Map<String, HikariDataSource> dataSources) {
        HashMap<String, ResultSet> resultSets = new HashMap<>();
        dataSources.forEach((k, v) -> {
            try {
                Connection connection = v.getConnection();
                Statement statement = connection.createStatement();
                String sql = "show tables;";
                resultSets.put(k, statement.executeQuery(sql));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        return resultSets;
    }

    public static Map<String, Map<String, ResultSet>> select(Map<String, HikariDataSource> dataSources, Map<String, List<String>> tables) {
        Map<String, Map<String, ResultSet>> result = new HashMap<>();
        dataSources.forEach((k, v) -> {
            Map<String, ResultSet> resultSets = new HashMap<>();
            try {
                tables.get(k).forEach(each -> {
                    try {
                        Connection connection = v.getConnection();
                        Statement statement = connection.createStatement();
                        String tableName = each.split(":")[1];
                        String sql = "select * from " + tableName + ";";
                        resultSets.put(tableName, statement.executeQuery(sql));
                    }
                    catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                result.put(k, resultSets);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return result;
    }

    public static Map<String, List<String>> printResultSet(final Map<String, ResultSet> resultSets) {
        Map<String, List<String>> result = new HashMap<>();
        resultSets.forEach((k, v) -> {
            if (v != null) {
                try {
                    List<String> resultValues = new ArrayList<>();
                    ResultSetMetaData metaData = v.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (v.next()) {
                        StringBuilder resultBuilder = new StringBuilder();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object values = v.getObject(i);
                            resultBuilder.append(MessageFormat.format("{0}:{1}", columnName, values));
                        }
                        resultValues.add(String.valueOf(resultBuilder));
                    }
                    result.put(k, resultValues);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        return result;
    }

    private static Optional<String> computeMD5(final String data) {
        if(null == data || data.length() == 0) {
            return Optional.empty();
        }
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(data.getBytes());
            byte[] digest = md5.digest();
            char[] hexDigits = {'0','1','2','3','4','5','6','7','8','9', 'a','b','c','d','e','f'};
            char[] chars = new char[digest.length * 2];
            int index = 0;
            for (byte b : digest) {
                chars[index++] = hexDigits[b >>> 4 & 0xf];
                chars[index++] = hexDigits[b & 0xf];
            }
            return Optional.of(new String(chars));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @SuppressWarnings("all")
    public static boolean isEqual(List<String> masterResults, List<String> slaveResults) {
        if (masterResults.size() != slaveResults.size()) {
            return false;
        }
        List<List<String>> values = new ArrayList<>();
        values.add(masterResults);
        values.add(slaveResults);
        StringBuilder md5;
        md5 = values.stream().map(each -> each.stream().reduce(new StringBuilder(), (a, b) -> a.append(computeMD5(b)),(a , b) -> null))
                .reduce((a, b) -> new StringBuilder("" + computeMD5(String.valueOf(a)).equals(computeMD5(String.valueOf(b))))).<RuntimeException>orElseThrow(() -> {throw new RuntimeException("未知异常");});
        return "true".contentEquals(md5);
    }

    public static void main(String[] args){
        Map<String, HikariConfig> config = Test.getConfig();
        Map<String, HikariDataSource> dataSource = Test.getDataSource(config);
        Map<String, ResultSet> pendingTables = Test.selectTables(dataSource);
        Map<String, List<String>> tables = Test.printResultSet(pendingTables);
        Map<String, HikariDataSource> dataSource2 = Test.getDataSource(config);
        Map<String, Map<String, ResultSet>> pendingResultSet = Test.select(dataSource2, tables);
        Map<String, List<String>> data = new HashMap<>();
        ArrayList<Boolean> booleans = new ArrayList<>();
        pendingResultSet.forEach((k, v) -> {
            Map<String, List<String>> pendingData = Test.printResultSet(v);
            pendingData.forEach((key, values) -> {
                values.forEach(System.out::println);
                if (data.containsKey(key)) {
                    booleans.add(Test.isEqual(data.get(key), values));
                } else {
                    data.put(key, values);
                }
            });
        });
        booleans.forEach(System.out::println);
        dataSource.forEach((k, v) -> {
            if (!v.isClosed()) {
                v.close();
            }
        });
        dataSource2.forEach((k, v) -> {
            if (!v.isClosed()) {
                v.close();
            }
        });
    }
}
