package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;

import java.sql.SQLException;
import java.util.List;

public class TableDataHandler extends Handler {

    public TableDataHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateCopySQL(final ExecuteContext context) throws SQLException {
        List<String> oldColumns = context.getOldColumns();
        List<String> newColumns = context.getNewColumns();
        String newTableName = context.getNewTableName();
        AlterStatement alterStatement = context.getAlterStatement();
        String tableName = alterStatement.getTableName();
        return getCopySQL(oldColumns, newColumns, newTableName, tableName);
    }

    private String getCopySQL(final List<String> oldColumns, final List<String> newColumns, final String newTableName, final String tableName) {
        String selectSQL = getSelectSQL(oldColumns, tableName);
        return getSubCopySQL(newColumns, newTableName, selectSQL);
    }

    private String getSelectSQL(final List<String> columns, final String tableName) {
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        return String.format("select %s from %s", columnNames, tableName);
    }

    private String getSubCopySQL(final List<String> columns, final String newTableName, final String selectSQL) {
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        return String.format("insert into %s(%s) (%s);", newTableName, columnNames, selectSQL);
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
