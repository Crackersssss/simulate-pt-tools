package com.cracker.pt.tablechecksum;

import com.cracker.pt.core.config.Config;
import com.cracker.pt.core.config.JDBCProps;
import com.cracker.pt.tablechecksum.core.Compute;
import com.cracker.pt.tablechecksum.query.QueryColumns;
import com.cracker.pt.tablechecksum.query.QueryTables;
import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.core.database.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Test2 {

    public static void main(String[] args) {
        Properties properties = new Properties();
        String url = "jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config resource_0 = new Config("resource_0", properties);
        ArrayList<Config> configs = new ArrayList<>();
        configs.add(resource_0);
        Properties properties2 = new Properties();
        String url2 = "jdbc:mysql://127.0.0.1:3306/pt_db2?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties2.setProperty(JDBCProps.JDBCURL.getPropName(), url2);
        properties2.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties2.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config resource_1 = new Config("resource_1", properties2);
        configs.add(resource_1);
        List<DataSource> dataSources = new ArrayList<>();
        configs.forEach(each -> dataSources.add(new DataSource(each.getDataSourceName(), each.getHikariConfig())));
        List<Table> tables = QueryTables.queryData(dataSources);
        Map<String, List<String>> data = QueryColumns.queryData(dataSources, tables);
        Map<String, Boolean> result = Compute.isEqual(data);
        result.forEach((k, v) -> {
            System.out.println(k + ":" + v);
        });
    }
}
