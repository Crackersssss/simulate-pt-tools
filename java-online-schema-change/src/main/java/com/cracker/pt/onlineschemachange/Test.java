package com.cracker.pt.onlineschemachange;

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

public class Test {

    private static List<HikariConfig> getConfig() {
        ArrayList<HikariConfig> configs = new ArrayList<>();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false");
        config.setUsername("root");
        config.setPassword("root$123");
        configs.add(config);
//        HikariConfig config2 = new HikariConfig();
//        config2.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/pt_db2?useUnicode=true&characterEncoding=utf8&useSSL=false");
//        config2.setUsername("root");
//        config2.setPassword("root$123");
//        configs.add(config2);
        return configs;
    }

    private static List<HikariDataSource> getDataSource() {
        List<HikariDataSource> dataSources = new ArrayList<>();
        getConfig().forEach(each -> dataSources.add(new HikariDataSource(each)));
        return dataSources;
    }

    public static List<ResultSet> showCreateTable() {
        List<ResultSet> resultSets = new ArrayList<>();
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                String sql = "show create table pt_ddl;";
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

    public static void createTable(String sql) {
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
                if (!each.isClosed()) {
                    each.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public static void alterTableStruct(String sql) {
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
                if (!each.isClosed()) {
                    each.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public static List<String> selectALLColumns() {
        List<String> resultSets = new ArrayList<>();
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                ResultSet pt_ddl = connection.getMetaData().getTables(null, "%", "pt_ddl", new String[]{"TABLE"});
                while (pt_ddl.next()) {
                    String tableName = pt_ddl.getString("TABLE_NAME");
                    System.out.println(tableName);

                    if (tableName.equals("pt_ddl")) {
                        ResultSet rs = connection.getMetaData().getColumns(null, "%", tableName.toUpperCase(), "%");

                        while (rs.next()) {
                            String colName = rs.getString("COLUMN_NAME");
                            resultSets.add(colName);
                        }
                    }
                }
                if (!each.isClosed()) {
                    each.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        return resultSets;
    }

    public static List<String> getAllColumns(final List<ResultSet> resultSets) {
        List<String> result = new ArrayList<>();
        resultSets.forEach(each -> {
            if (each != null) {
                try {
                    ResultSetMetaData metaData = each.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (each.next()) {
                        //StringBuilder resultBuilder = new StringBuilder();
                        for (int i = 0; i < columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            result.add(String.valueOf(columnName));
                        }
                        //Object values = each.getObject(2);
                        //resultBuilder.append(MessageFormat.format("{0}", values));
                        //result.add(String.valueOf(resultBuilder));
                    }
                    each.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        return result;
    }

    private static void copyData(String sql) {
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
                if (!each.isClosed()) {
                    each.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    private static void deleteTable(String sql) {
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
                if (!each.isClosed()) {
                    each.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    private static void renameTable(String sql) {
        getDataSource().forEach(each -> {
            try {
                Connection connection = each.getConnection();
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
                if (!each.isClosed()) {
                    each.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public static void alterTable() {
        List<String> results = getCreateTableSQL(showCreateTable());
        //处理语句
        String s = results.get(0);
        String substring = s.substring(s.indexOf('`'), s.indexOf('`', s.indexOf('`') + 1) + 1);
        String replace = s.replace(substring, "`pt_ddl_pt_new`");
        //新建副表
        createTable(replace);
        //修改表结构
        String sql = "alter table pt_ddl_pt_new add test_columns varchar(20);";
        alterTableStruct(sql);
        //TODO: 创建触发器
        //拷贝数据
            //查询所有字段
        List<String> columnsResultSet = selectALLColumns();
            //生成语句
        String columnNames = columnsResultSet.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("未知错误"));
        String insertSQL = "insert into pt_ddl_pt_new( " + columnNames + ")" ;
        String selectSQL = "select ";
        selectSQL = selectSQL + columnNames;
        selectSQL = selectSQL + " from pt_ddl";
        insertSQL = insertSQL + " (" + selectSQL + ");";
        copyData(insertSQL);
        //重命名
            //删除原表
        deleteTable("drop table pt_ddl;");
        renameTable("rename table pt_ddl_pt_new to pt_ddl;");
    }

    public static List<String> getCreateTableSQL(final List<ResultSet> resultSets) {
        List<String> result = new ArrayList<>();
        resultSets.forEach(each -> {
            if (each != null) {
                try {
                    ResultSetMetaData metaData = each.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (each.next()) {
                        StringBuilder resultBuilder = new StringBuilder();
                        //String columnName = metaData.getColumnName(2);
                        Object values = each.getObject(2);
                        resultBuilder.append(MessageFormat.format("{0}", values));
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
//        List<String> results = printResultSet(showCreateTable());
//        results.forEach(System.out::println);
        alterTable();
    }
}
