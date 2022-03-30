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
        switch (execute) {
            case DELETE:
                sql = String.format("create trigger trigger_%s_del after %s on %s for each row begin ", tableName, execute, tableName);
                sql = sql + String.format("delete from %s where %s=old.%s; end", newTableName, primaryKey, primaryKey);
                break;
            case UPDATE:
                tableColumnNames = tableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                newTableColumnNames = newTableColumns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException(UNKNOWN_ERROR));
                sql = String.format("create trigger trigger_%s_upd after %s on %s for each row begin ", tableName, execute, tableName);
                sql = sql + String.format("delete from %s where %s=old.%s;", newTableName, primaryKey, primaryKey);
                sql = sql + String.format("insert into %s (%s) (select %s from %s where %s=old.%s); end", newTableName, tableColumnNames, newTableColumnNames, tableName, primaryKey, primaryKey);
                break;
            case INSERT:
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
