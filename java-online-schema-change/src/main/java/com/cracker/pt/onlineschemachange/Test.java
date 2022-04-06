package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.config.Config;
import com.cracker.pt.core.config.JDBCProps;
import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;

import java.text.SimpleDateFormat;
import java.util.Properties;

public final class Test {

    private static DataSource dataSource;

    private static void init() {
        Properties properties = new Properties();
        String url = "jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config config = new Config("resource_0", properties);
        dataSource = new DataSource(config);
    }

    private static AlterStatement getAddAlterStatement() {
        init();
        String tableName = "pt_ddl";
        String alterType = "add";
        String columnName = "test_columns";
        String columnType = "varchar(20)";
        return new AlterStatement(tableName, alterType, columnName, columnType);
    }

    private static AlterStatement getDropAlterStatement() {
        init();
        String tableName = "pt_ddl";
        String alterType = "drop";
        String columnName = "test_columns";
        return new AlterStatement(tableName, alterType, columnName);
    }

    private static AlterStatement getChangeAlterStatement() {
        init();
        String tableName = "pt_ddl";
        String alterType = "change";
        String columnName = "test_columns";
        String newColumnName = "bbb";
        String columnType = "char(30)";
        return new AlterStatement(tableName, alterType, columnName, newColumnName, columnType);
    }

    private static AlterStatement getChangeAlterStatement2() {
        init();
        String tableName = "pt_ddl";
        String alterType = "change";
        String columnName = "bbb";
        String newColumnName = "test_columns";
        String columnType = "varchar(30)";
        return new AlterStatement(tableName, alterType, columnName, newColumnName, columnType);
    }

    public static void main(final String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(System.currentTimeMillis()));
//        AlterStatement alterStatement = getAddAlterStatement();
//        AlterStatement alterStatement = getDropAlterStatement();
        AlterStatement alterStatement = getChangeAlterStatement();
        Execute execute = new Execute(dataSource);
        execute.alterTable(alterStatement);
        System.out.println(df.format(System.currentTimeMillis()));
//        AlterStatement alterStatement2 = getDropAlterStatement();
        AlterStatement alterStatement2 = getChangeAlterStatement2();
        Execute execute2 = new Execute(dataSource);
        execute2.alterTable(alterStatement2);
        System.out.println(df.format(System.currentTimeMillis()));
    }
}
