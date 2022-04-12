package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;

import java.sql.SQLException;
import java.util.List;

/**
 * Table data operating handler.
 */
public class TableDataHandler extends Handler {

    public TableDataHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateCopySQL(final ExecuteContext context) {
        List<String> oldColumns = context.getOldColumns();
        List<String> newColumns = context.getNewColumns();
        return getCopySQL(oldColumns, newColumns, context);
    }

    private String getCopySQL(final List<String> oldColumns, final List<String> newColumns, final ExecuteContext context) {
        String selectSQL = getSelectSQL(oldColumns, context);
        return getSubCopySQL(newColumns, selectSQL, context);
    }

    private String getSelectSQL(final List<String> columns, final ExecuteContext context) {
        String tableName = context.getAlterStatement().getTableName();
        String primaryKey = context.getPrimaryKey();
        String copyStartIndex = context.getCopyStartIndex();
        String copyEndIndex = context.getCopyEndIndex();
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        return String.format("select %s from %s where %s >= '%s' and %s <= '%s'",
                columnNames, tableName, primaryKey, copyStartIndex, primaryKey, copyEndIndex);
    }

    private String getSubCopySQL(final List<String> columns, final String selectSQL, final ExecuteContext context) {
        String newTableName = context.getNewTableName();
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        return String.format("REPLACE into %s(%s) (%s);", newTableName, columnNames, selectSQL);
    }

    public void copyData(final String sql) throws SQLException {
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
