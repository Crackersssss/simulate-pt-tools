package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.utils.TriggerType;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TableTriggerHandler extends Handler {

    private static final String COLUMN_NAME = "COLUMN_NAME";

    private static final String UNKNOWN_ERROR = "unknown error";

    public TableTriggerHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void createTrigger(final String tableName, final String newTableName, final TableColumnsHandler columnsHandler) throws SQLException {
        DatabaseMetaData metaData = getConnection().getMetaData();
        ResultSet resultSet = metaData.getPrimaryKeys(null, null, newTableName);
        String primaryKey = null;
        while (resultSet.next()) {
            primaryKey = resultSet.getString(COLUMN_NAME);
        }
        if (null == primaryKey) {
            throw new OnlineDDLException("Table %s has no primary key", tableName);
        }
        String sql = getCreateTriggerSQL(tableName, newTableName, primaryKey, TriggerType.DELETE, columnsHandler);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(tableName, newTableName, primaryKey, TriggerType.UPDATE, columnsHandler);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(tableName, newTableName, primaryKey, TriggerType.INSERT, columnsHandler);
        getStatement().executeUpdate(sql);
    }

    private String getCreateTriggerSQL(final String tableName, final String newTableName, final String primaryKey, final TriggerType execute, final TableColumnsHandler columnsHandler) throws SQLException {
        List<String> tableColumns;
        List<String> newTableColumns;
        String tableColumnNames;
        String newTableColumnNames;
        String sql;
        switch (execute) {
            case DELETE:
                sql = String.format("create trigger trigger_%s_del after %s on %s for each row begin ", tableName, execute, tableName);
                sql = sql + String.format("delete from %s where %s=old.%s; end", newTableName, primaryKey, primaryKey);
                break;
            case UPDATE:
                tableColumns = columnsHandler.getAllColumns(tableName);
                newTableColumns = columnsHandler.getAllColumns(newTableName);
                tableColumnNames = tableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                newTableColumnNames = newTableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                sql = String.format("create trigger trigger_%s_upd after %s on %s for each row begin ", tableName, execute, tableName);
                sql = sql + String.format("delete from %s where %s=old.%s;", newTableName, primaryKey, primaryKey);
                sql = sql + String.format("insert into %s (%s) (select %s from %s where %s=old.%s); end", newTableName, tableColumnNames, newTableColumnNames, tableName, primaryKey, primaryKey);
                break;
            case INSERT:
                tableColumns = columnsHandler.getAllColumns(tableName);
                newTableColumns = columnsHandler.getAllColumns(newTableName);
                tableColumnNames = tableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                newTableColumnNames = newTableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
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
