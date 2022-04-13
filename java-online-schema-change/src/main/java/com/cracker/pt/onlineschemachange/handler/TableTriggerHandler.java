package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import com.cracker.pt.onlineschemachange.utils.AlterType;
import com.cracker.pt.onlineschemachange.utils.TriggerType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

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
        List<String> primaryKey = new ArrayList<>();
        while (resultSet.next()) {
            primaryKey.add(resultSet.getString(COLUMN_NAME));
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

    private String getCreateTriggerSQL(final ExecuteContext context, final List<String> primaryKey,
                                       final TriggerType execute) {
        List<String> tableColumns = context.getOldColumns();
        String tableName = context.getAlterStatement().getTableName();
        String shardowTableName = context.getNewTableName();
        String database = getDatabaseName();
        String sql;

        StringJoiner columnStr = new StringJoiner(",");
        for (String s : tableColumns) {
            columnStr.add(s);
        }

        //获取NEW字段
        StringJoiner NewColumnStr = new StringJoiner(",NEW.", "NEW.", "");
        for (String s : tableColumns) {
            NewColumnStr.add(s);
        }

        StringJoiner pkStr = new StringJoiner(",");
        for (String s : primaryKey) {
            pkStr.add(s);
        }

        //获取OLD主键
        StringJoiner oldPKStr = new StringJoiner(",OLD.", "OLD.", "");

        for (String s : primaryKey) {
            oldPKStr.add(s);
        }

        switch (execute) {
            case DELETE:
                sql = String.format("CREATE TRIGGER `%s`.`trigger_%s_del` AFTER DELETE ON %s.%s FOR EACH ROW DELETE " +
                        "FROM %s.%s WHERE (%s) = (%s)",database,tableName,database,tableName,database,
                        shardowTableName,pkStr,oldPKStr);
                break;
            case UPDATE:
                sql = String.format("CREATE TRIGGER `%s`.`trigger_%s_upd` AFTER UPDATE ON  %s.%s FOR EACH ROW BEGIN \n" +
                        " DELETE FROM %s.%s WHERE (%s) = (%s); \n" +
                        " REPLACE INTO %s.%s (%s) \n" + "VALUES (%s);END",database,tableName,database,tableName,
                        database,shardowTableName,pkStr,oldPKStr,database,shardowTableName,columnStr,NewColumnStr);
                break;
            case INSERT:
                sql = String.format("CREATE TRIGGER `%s`.`trigger_%s_ins` AFTER INSERT ON %s.%s FOR EACH ROW REPLACE " +
                        "INTO %s.%s (%s) VALUES (%s)",database,tableName,database,tableName,database,shardowTableName
                        ,columnStr,NewColumnStr);
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
        String sql = String.format("drop trigger %s", context.getDeleteTrigger());
        getStatement().executeUpdate(sql);
        sql = String.format("drop trigger %s", context.getUpdateTrigger());
        getStatement().executeUpdate(sql);
        sql = String.format("drop trigger %s", context.getInsertTrigger());
        String deleteTrigger = context.getDeleteTrigger();
        String updateTrigger = context.getUpdateTrigger();
        String insertTrigger = context.getInsertTrigger();
        dropTrigger(deleteTrigger);
        dropTrigger(updateTrigger);
        dropTrigger(insertTrigger);
    }

    private void dropTrigger(final String triggerName) throws SQLException {
        String sql = String.format("drop trigger if exist %s;", triggerName);
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
