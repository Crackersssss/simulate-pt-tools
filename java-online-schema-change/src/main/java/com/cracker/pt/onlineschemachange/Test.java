package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.config.Config;
import com.cracker.pt.core.config.JDBCProps;
import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.handler.TableAlterHandler;
import com.cracker.pt.onlineschemachange.handler.TableColumnsHandler;
import com.cracker.pt.onlineschemachange.handler.TableCreateHandler;
import com.cracker.pt.onlineschemachange.handler.TableDataHandler;
import com.cracker.pt.onlineschemachange.handler.TableDropHandler;
import com.cracker.pt.onlineschemachange.handler.TableRenameHandler;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;

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

    public static void main(String[] args) {
        Config config = getConfig();
        DataSource dataSource = new DataSource(config);
        String tableName = "pt_ddl";
        String alterType = "add";
        String columnName = "test_columns";
        String columnType = "varchar(20)";
        AlterStatement alterStatement = new AlterStatement(tableName, alterType, columnName, columnType);
        String newTableName;
        TableCreateHandler createHandler;
        TableAlterHandler alterHandler;
        TableDataHandler dataHandler;
        TableDropHandler dropHandler;
        TableRenameHandler renameHandler;
        try {
            createHandler = new TableCreateHandler(dataSource);
            createHandler.createTable(alterStatement);
            newTableName = createHandler.getNewTableName();
            alterHandler = new TableAlterHandler(dataSource);
            String alterSQL = alterHandler.generateAlterSQL(alterStatement, newTableName);
            alterHandler.alterTableStruct(alterSQL);
            //TODO 创建触发器
            TableColumnsHandler columnsHandler = new TableColumnsHandler(dataSource);
            List<String> columns = columnsHandler.getAllColumns(tableName);
            dataHandler = new TableDataHandler(dataSource);
            String copySQL = dataHandler.generateCopySQL(columns, tableName, newTableName);
            dataHandler.copyData(copySQL);
            dropHandler = new TableDropHandler(dataSource);
            String dropSQL = dropHandler.generateDropSQL(tableName);
            dropHandler.deleteTable(dropSQL);
            renameHandler = new TableRenameHandler(dataSource);
            String renameSQL = renameHandler.generateRenameSQL(newTableName, tableName);
            renameHandler.renameTable(renameSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (!dataSource.getHikariDataSource().isClosed()) {
                dataSource.getHikariDataSource().close();
            }
        }

    }
}
