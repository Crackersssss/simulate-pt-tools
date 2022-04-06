package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import com.cracker.pt.onlineschemachange.utils.AlterType;
import com.cracker.pt.onlineschemachange.utils.TriggerType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Table trigger operating handler.
 */
public class TableTriggerHandler extends Handler {

    private static final String COLUMN_NAME = "COLUMN_NAME";

    private static final String UNKNOWN_ERROR = "unknown error";

    public TableTriggerHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void createTrigger(final TableColumnsHandler columnsHandler, final ExecuteContext context) throws SQLException {
        ResultSet resultSet = getConnection().getMetaData().getPrimaryKeys(null, null, context.getNewTableName());
        String primaryKey = null;
        while (resultSet.next()) {
            primaryKey = resultSet.getString(COLUMN_NAME);
        }
        if (null == primaryKey) {
            throw new OnlineDDLException("Table %s has no primary key", context.getAlterStatement().getTableName());
        }
        context.setPrimaryKey(primaryKey);
        getColumns(columnsHandler, context);
        String sql = getCreateTriggerSQL(context, primaryKey, TriggerType.DELETE);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(context, primaryKey, TriggerType.UPDATE);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(context, primaryKey, TriggerType.INSERT);
        getStatement().executeUpdate(sql);
    }

    private String getCreateTriggerSQL(final ExecuteContext context, final String primaryKey, final TriggerType execute) {
        List<String> tableColumns = context.getOldColumns();
        List<String> newTableColumns = context.getNewColumns();
        String tableName = context.getAlterStatement().getTableName();
        String newTableName = context.getNewTableName();
        String tableColumnNames;
        String newTableColumnNames;
        String sql;
        String triggerName;
        switch (execute) {
            case DELETE:
                triggerName = String.format("trigger_%s_del", tableName);
                context.setDeleteTrigger(triggerName);
                sql = getSQLHead(triggerName, execute, tableName) + String.format("delete from %s where %s=old.%s; end", newTableName, primaryKey, primaryKey);
                break;
            case UPDATE:
                tableColumnNames = tableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                newTableColumnNames = newTableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                triggerName = String.format("trigger_%s_upd", tableName);
                context.setUpdateTrigger(triggerName);
                sql = getSQLHead(triggerName, execute, tableName) + String.format("delete from %s where %s=old.%s;", newTableName, primaryKey, primaryKey);
                sql = sql + String.format("REPLACE into %s (%s) (select %s from %s where %s=old.%s); end", newTableName, newTableColumnNames, tableColumnNames, tableName, primaryKey, primaryKey);
                break;
            case INSERT:
                tableColumnNames = tableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                newTableColumnNames = newTableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                triggerName = String.format("trigger_%s_ins", tableName);
                context.setInsertTrigger(triggerName);
                sql = getSQLHead(triggerName, execute, tableName)
                        + String.format("REPLACE into %s (%s) (select %s from %s where %s=new.%s); end", newTableName, newTableColumnNames, tableColumnNames, tableName, primaryKey, primaryKey);
                break;
            default:
                throw new OnlineDDLException("Unable to create trigger of type %s", execute);
        }
        return sql;
    }

    private String getSQLHead(final String triggerName, final TriggerType execute, final String tableName) {
        return String.format("create trigger %s after %s on %s for each row begin ", triggerName, execute, tableName);
    }

    public void getColumns(final TableColumnsHandler columnsHandler, final ExecuteContext context) throws SQLException {
        List<String> columns;
        String newTableName = context.getNewTableName();
        AlterStatement alterStatement = context.getAlterStatement();
        String alterType = alterStatement.getAlterType();
        String tableName = alterStatement.getTableName();
        switch (AlterType.valueOf(alterType.toUpperCase())) {
            case ADD:
                columns = columnsHandler.getAllColumns(tableName);
                context.setOldColumns(columns);
                context.setNewColumns(columns);
                break;
            case DROP:
                columns = columnsHandler.getAllColumns(newTableName);
                context.setOldColumns(columns);
                context.setNewColumns(columns);
                break;
            case CHANGE:
                List<String> oldColumns = columnsHandler.getAllColumns(tableName);
                List<String> newColumns = columnsHandler.getAllColumns(newTableName);
                context.setOldColumns(oldColumns);
                context.setNewColumns(newColumns);
                break;
            default:
                throw new OnlineDDLException("Operation %s is not supported!", alterType);
        }
    }

    public void dropAllTrigger(final ExecuteContext context) throws SQLException {
        String sql = String.format("drop trigger %s, %s, %s;", context.getDeleteTrigger(), context.getUpdateTrigger(), context.getInsertTrigger());
        getStatement().executeUpdate(sql);
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
