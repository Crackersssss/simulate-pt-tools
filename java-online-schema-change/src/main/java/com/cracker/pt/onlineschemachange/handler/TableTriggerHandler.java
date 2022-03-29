package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TableTriggerHandler extends Handler {

    public TableTriggerHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void createTrigger(final String tableName, final String newTableName, final TableColumnsHandler columnsHandler) throws SQLException {
        DatabaseMetaData metaData = getConnection().getMetaData();
        ResultSet resultSet = metaData.getPrimaryKeys(null, null, newTableName);
        String primaryKey = null;
        while (resultSet.next()) {
            primaryKey = resultSet.getString("COLUMN_NAME");
        }
        String sql = getCreateTriggerSQL(tableName, newTableName, primaryKey, "delete", columnsHandler);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(tableName, newTableName, primaryKey, "update", columnsHandler);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(tableName, newTableName, primaryKey, "insert", columnsHandler);
        getStatement().executeUpdate(sql);
    }

    private String getCreateTriggerSQL(final String tableName, final String newTableName, final String primaryKey, final String execute, final TableColumnsHandler columnsHandler) throws SQLException {
        List<String> tableColumns;
        List<String> newTableColumns;
        String tableColumnNames;
        String newTableColumnNames;
        String sql;
        switch (execute) {
            case "delete":
                sql = String.format("create trigger trigger_%s_del after %s on %s for each row begin ", tableName, execute, tableName);
                sql = sql + String.format("delete from %s where %s=old.%s; end", newTableName, primaryKey, primaryKey);
                break;
            case "update":
                tableColumns = columnsHandler.getAllColumns(tableName);
                newTableColumns = columnsHandler.getAllColumns(newTableName);
                tableColumnNames = tableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
                newTableColumnNames = newTableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
                sql = String.format("create trigger trigger_%s_upd after %s on %s for each row begin ", tableName, execute, tableName);
                sql = sql + String.format("delete from %s where %s=old.%s;", newTableName, primaryKey, primaryKey);
                sql = sql + String.format("insert into %s (%s) (select %s from %s where %s=old.%s); end", newTableName, tableColumnNames, newTableColumnNames, tableName, primaryKey, primaryKey);
                break;
            case "insert":
                tableColumns = columnsHandler.getAllColumns(tableName);
                newTableColumns = columnsHandler.getAllColumns(newTableName);
                tableColumnNames = tableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
                newTableColumnNames = newTableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
                sql = String.format("create trigger trigger_%s_ins after %s on %s for each row begin ", tableName, execute, tableName);
                sql = sql + String.format("insert into %s (%s) (select %s from %s where %s=new.%s); end", newTableName, tableColumnNames, newTableColumnNames, tableName, primaryKey, primaryKey);
                break;
            default:
                throw new OnlineDDLException("Unable to create trigger of type %s", execute);
        }
        return sql;
    }

    @Override
    public void begin() throws SQLException {
        super.begin();
    }

    @Override
    public void commit() throws SQLException {
        super.commit();
    }

    @Override
    public void close() throws SQLException {
        super.close();
    }

    @Override
    public void rollback() throws SQLException {
        super.rollback();
    }
}
