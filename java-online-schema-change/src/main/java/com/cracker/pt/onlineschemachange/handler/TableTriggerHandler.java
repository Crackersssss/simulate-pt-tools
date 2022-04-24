package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import com.cracker.pt.onlineschemachange.utils.AlterType;
import com.cracker.pt.onlineschemachange.utils.TriggerType;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;
import java.util.StringJoiner;

/**
 * Table trigger operating handler.
 */
@Slf4j
public class TableTriggerHandler extends Handler {

    public TableTriggerHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void createTrigger(final TableColumnsHandler columnsHandler, final ExecuteContext context) throws SQLException {
        columnsHandler.setPrimaryKeys(context);
        getColumns(columnsHandler, context);
        String sql = getCreateTriggerSQL(context, TriggerType.DELETE);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(context, TriggerType.UPDATE);
        getStatement().executeUpdate(sql);
        sql = getCreateTriggerSQL(context, TriggerType.INSERT);
        getStatement().executeUpdate(sql);
    }

    private String getCreateTriggerSQL(final ExecuteContext context,
                                       final TriggerType execute) {
        List<String> oldColumns = context.getOldColumns();
        List<String> newColumns = context.getNewColumns();
        String tableName = context.getAlterStatement().getTableName();
        String newTableName = context.getNewTableName();
        String databaseName = getDatabaseName();
        //获取NEW字段
        StringJoiner newColumnStr = newColumns.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        StringJoiner oldColumnStr = oldColumns.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        StringJoiner oldNewColumnStr = oldColumns.stream().reduce(new StringJoiner(", new.", "new.", ""), StringJoiner::add, (a, b) -> null);
        StringJoiner oldOldColumnStr = oldColumns.stream().reduce(new StringJoiner(", old.", "old.", ""), StringJoiner::add, (a, b) -> null);
        List<String> primaryKeys = context.getPrimaryKeys();
        StringJoiner pkStr = primaryKeys.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        //获取OLD主键
        StringJoiner oldPKStr = primaryKeys.stream().reduce(new StringJoiner(", old.", "old.", ""), StringJoiner::add, (a, b) -> null);
        String sql;
        switch (execute) {
            case DELETE:
                sql = String.format("CREATE TRIGGER `%s`.`trigger_%s_del` AFTER DELETE ON %s.%s FOR EACH ROW BEGIN DELETE FROM %s.%s WHERE (%s) = (%s);END",
                        databaseName, tableName, databaseName, tableName, databaseName,
                        newTableName, pkStr, oldPKStr);
                break;
            case UPDATE:
                sql = String.format("CREATE TRIGGER `%s`.`trigger_%s_upd` AFTER UPDATE ON  %s.%s FOR EACH ROW BEGIN "
                                + " DELETE FROM %s.%s WHERE (%s) = (%s); REPLACE INTO %s.%s (%s) (select %s from %s where %s = %s);END",
                        databaseName, tableName, databaseName, tableName,
                        databaseName, newTableName, pkStr, oldPKStr, databaseName, newTableName, newColumnStr, oldColumnStr, tableName, pkStr, oldPKStr);
                break;
            case INSERT:
                sql = String.format("CREATE TRIGGER `%s`.`trigger_%s_ins` AFTER INSERT ON %s.%s FOR EACH ROW REPLACE INTO %s.%s (%s) VALUES (%s)",
                        databaseName, tableName, databaseName, tableName, databaseName, newTableName, newColumnStr, oldNewColumnStr);
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

    public void dropAllTrigger(final ExecuteContext context) throws SQLException {
        String deleteTrigger = context.getDeleteTrigger();
        String updateTrigger = context.getUpdateTrigger();
        String insertTrigger = context.getInsertTrigger();
        dropTrigger(deleteTrigger);
        dropTrigger(updateTrigger);
        dropTrigger(insertTrigger);
    }

    public void dropTrigger(final String triggerName) throws SQLException {
        String sql = String.format("drop trigger if exists %s;", triggerName);
        getStatement().executeUpdate(sql);
    }
}
