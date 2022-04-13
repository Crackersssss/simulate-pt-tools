package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.config.Config;
import com.cracker.pt.core.config.JDBCProps;
import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteDatasource;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;

@Slf4j
public final class Test {

    private static DataSource dataSource;

    private static void init() {
        Properties properties = new Properties();
        String url = "jdbc:mysql://172.18.40.126:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), "pcloud");
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), "pcloud");
        Config config = new Config("resource_0", properties);
        dataSource = new DataSource(config, "test");
    }

    private static AlterStatement getAddAlterStatement() {
        init();
        String tableName = "test";
        String alterType = "add";
        String columnName = "test_columns";
        String columnType = "varchar(20)";
        return new AlterStatement(tableName, alterType, columnName, columnType);
    }

    private static AlterStatement getDropAlterStatement() {
        init();
        String tableName = "test";
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

    private static void testShardingTable1() {
        Properties properties = new Properties();
        String url = "jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config config = new Config("resource_0", properties);

        String tableName3 = "pt_table1";
        String alterType3 = "add";
        String columnName3 = "test_columns";
        String columnType3 = "varchar(30)";
        ArrayList<ExecuteDatasource> executeDatasource = new ArrayList<>();
        AlterStatement alterStatement1 = new AlterStatement(tableName3, alterType3, columnName3, columnType3);
        executeDatasource.add(new ExecuteDatasource(alterStatement1, new DataSource(config, "pt_db")));
        String tableName4 = "pt_table2";
        String alterType4 = "add";
        String columnName4 = "test_columns";
        String columnType4 = "varchar(30)";
        AlterStatement alterStatement2 = new AlterStatement(tableName4, alterType4, columnName4, columnType4);
        executeDatasource.add(new ExecuteDatasource(alterStatement2, new DataSource(config, "pt_db")));
        Execute execute = new Execute(executeDatasource);
        execute.alterTable();
    }

    private static void testShardingTable2() {
        Properties properties = new Properties();
        String url = "jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config config = new Config("resource_0", properties);

        String tableName3 = "pt_table1";
        String alterType3 = "drop";
        String columnName3 = "test_columns";
        ArrayList<ExecuteDatasource> executeDatasource = new ArrayList<>();
        AlterStatement alterStatement1 = new AlterStatement(tableName3, alterType3, columnName3);
        executeDatasource.add(new ExecuteDatasource(alterStatement1, new DataSource(config, "pt_db")));
        String tableName4 = "pt_table2";
        String alterType4 = "drop";
        String columnName4 = "test_columns";
        AlterStatement alterStatement2 = new AlterStatement(tableName4, alterType4, columnName4);
        executeDatasource.add(new ExecuteDatasource(alterStatement2, new DataSource(config, "pt_db")));
        Execute execute = new Execute(executeDatasource);
        execute.alterTable();
    }

    private static void testShardingTable3() {
        Properties properties = new Properties();
        String url = "jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config config = new Config("resource_0", properties);

        String tableName3 = "pt_table1";
        String alterType3 = "change";
        String columnName3 = "alter_column";
        String newColumnName3 = "bbb";
        String columnType3 = "char(30)";
        ArrayList<ExecuteDatasource> executeDatasource = new ArrayList<>();
        AlterStatement alterStatement1 = new AlterStatement(tableName3, alterType3, columnName3, newColumnName3, columnType3);
        executeDatasource.add(new ExecuteDatasource(alterStatement1, new DataSource(config, "pt_db")));
        String tableName4 = "pt_table2";
        String alterType4 = "change";
        String columnName4 = "alter_column";
        String newColumnName4 = "bbb";
        String columnType4 = "char(30)";
        AlterStatement alterStatement2 = new AlterStatement(tableName4, alterType4, columnName4, newColumnName4, columnType4);
        executeDatasource.add(new ExecuteDatasource(alterStatement2, new DataSource(config, "pt_db")));
        Execute execute = new Execute(executeDatasource);
        execute.alterTable();
    }

    private static void testShardingTable4() {
        Properties properties = new Properties();
        String url = "jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config config = new Config("resource_0", properties);

        String tableName3 = "pt_table1";
        String alterType3 = "change";
        String columnName3 = "bbb";
        String newColumnName3 = "alter_column";
        String columnType3 = "char(30)";
        ArrayList<ExecuteDatasource> executeDatasource = new ArrayList<>();
        AlterStatement alterStatement1 = new AlterStatement(tableName3, alterType3, columnName3, newColumnName3, columnType3);
        executeDatasource.add(new ExecuteDatasource(alterStatement1, new DataSource(config, "pt_db")));
        String tableName4 = "pt_table2";
        String alterType4 = "change";
        String columnName4 = "bbb";
        String newColumnName4 = "alter_column";
        String columnType4 = "char(30)";
        AlterStatement alterStatement2 = new AlterStatement(tableName4, alterType4, columnName4, newColumnName4, columnType4);
        executeDatasource.add(new ExecuteDatasource(alterStatement2, new DataSource(config, "pt_db")));
        Execute execute = new Execute(executeDatasource);
        execute.alterTable();
    }

    public static void main(final String[] args) throws InterruptedException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("start = {}", df.format(System.currentTimeMillis()));
        testShardingTable1();
//        testShardingTable3();
        log.info("end = {}", df.format(System.currentTimeMillis()));
        log.info("start = {}", df.format(System.currentTimeMillis()));
        testShardingTable2();
//        testShardingTable4();
        log.info("end = {}", df.format(System.currentTimeMillis()));
    }
}
