package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.config.Config;
import com.cracker.pt.core.config.JDBCProps;
import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.handler.TableAlterHandler;
import com.cracker.pt.onlineschemachange.handler.TableColumnsHandler;
import com.cracker.pt.onlineschemachange.handler.TableCreateHandler;
import com.cracker.pt.onlineschemachange.handler.TableDataHandler;
import com.cracker.pt.onlineschemachange.handler.TableDeleteHandler;
import com.cracker.pt.onlineschemachange.handler.TableRenameHandler;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class Test {

    private static Config getConfig() {
        Properties properties = new Properties();
        String url = "jdbc:mysql://127.0.0.1:3306/pt_db?useUnicode=true&characterEncoding=utf8&useSSL=false";
        properties.setProperty(JDBCProps.JDBCURL.getPropName(), url);
        properties.setProperty(JDBCProps.USERNAME.getPropName(), JDBCProps.USERNAME.getDefaultValue());
        properties.setProperty(JDBCProps.PASSWORD.getPropName(), JDBCProps.PASSWORD.getDefaultValue());
        Config config = new Config("resource_0", properties);
        return config;
    }

    public static void alterTable() throws SQLException {
        Config config = getConfig();
        DataSource dataSource = new DataSource(config);
        String tableName = "pt_ddl";
        //新建副表
        TableCreateHandler.createTable(dataSource, tableName);
        //修改表结构
        String newTableName = TableCreateHandler.getNewTableName();
        String alterStatement = "alter table " + newTableName + " add test_columns varchar(20);";
        TableAlterHandler.alterTableStruct(dataSource, alterStatement);
        //TODO 创建触发器
        //拷贝数据
            //查询所有字段
        List<String> columns = TableColumnsHandler.getAllColumns(dataSource);
            //生成语句
        String columnNames = columns.stream().limit(3).reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("未知错误"));
        String insertSQL = "insert into pt_ddl_pt_new( " + columnNames + ")";
        String selectSQL = "select ";
        selectSQL = selectSQL + columnNames;
        selectSQL = selectSQL + " from pt_ddl";
        insertSQL = insertSQL + " (" + selectSQL + ");";
        TableDataHandler.copyData(dataSource, insertSQL);
        //重命名
            //删除原表
        TableDeleteHandler.deleteTable(dataSource, "drop table pt_ddl;");
        TableRenameHandler.renameTable(dataSource, "rename table pt_ddl_pt_new to pt_ddl;");
        if (!dataSource.getHikariDataSource().isClosed()) {
            dataSource.getHikariDataSource().close();
        }
    }

    public static void main(String[] args) throws SQLException {
        alterTable();
    }
}
