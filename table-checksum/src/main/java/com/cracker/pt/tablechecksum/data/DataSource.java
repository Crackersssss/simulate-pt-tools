package com.cracker.pt.tablechecksum.data;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DataSource {

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

    private static Map<String, HikariDataSource> getDataSource() {
        Map<String, HikariDataSource> dataSources = new HashMap<>();
        getConfig().forEach((k, v) -> dataSources.put(k, new HikariDataSource(v)));
        return dataSources;
    }

    public static Map<String, ResultSet> select() {
        Map<String, ResultSet> resultSets = new HashMap<>();
        getDataSource().forEach((k, v) -> {
            try {
                Connection connection = v.getConnection();
                Statement statement = connection.createStatement();
                String sql = "select * from pt_table;";
                resultSets.put(k, statement.executeQuery(sql));
                if (!v.isClosed()) {
                    v.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        return resultSets;
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
                    v.close();
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
    public static boolean isEqual(Map<String, List<String>> results) {
        Object[] valuesArray = results.values().toArray();
        List<List<String>> values = Arrays.stream(valuesArray).map(each -> (List<String>)each).collect(Collectors.toList());
        if (values.get(0).size() != values.get(1).size()) {
            return false;
        }
        StringBuilder md5;
        md5 = values.stream().map(each -> each.stream().reduce(new StringBuilder(), (a, b) -> a.append(computeMD5(b)),(a , b) -> null))
                .reduce((a, b) -> new StringBuilder("" + computeMD5(String.valueOf(a)).equals(computeMD5(String.valueOf(b))))).<RuntimeException>orElseThrow(() -> {throw new RuntimeException("未知异常");});
        return "true".contentEquals(md5);
    }

    public static void main(String[] args){
        System.out.println(DataSource.isEqual(DataSource.printResultSet(DataSource.select())));
    }
}
