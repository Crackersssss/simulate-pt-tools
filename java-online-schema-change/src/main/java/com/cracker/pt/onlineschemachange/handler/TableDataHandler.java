package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.utils.AlterType;

import java.sql.SQLException;
import java.util.List;

public class TableDataHandler extends Handler {

    public TableDataHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateCopySQL(final TableColumnsHandler columnsHandler, final String alterType, final String tableName, final String newTableName) throws SQLException {
        String copySQL;
        List<String> columns;
        switch (AlterType.valueOf(alterType.toUpperCase())) {
            case ADD:
                columns = columnsHandler.getAllColumns(tableName);
                copySQL = getCopySQL(columns, columns, newTableName, tableName);
                break;
            case DROP:
                columns = columnsHandler.getAllColumns(newTableName);
                copySQL = getCopySQL(columns, columns, newTableName, tableName);
                break;
            case CHANGE:
                List<String> oldColumns = columnsHandler.getAllColumns(tableName);
                List<String> newColumns = columnsHandler.getAllColumns(newTableName);
                copySQL = getCopySQL(oldColumns, newColumns, newTableName, tableName);
                break;
            default:
                throw new OnlineDDLException("Operation %s is not supported!", alterType);
        }
        return copySQL;
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
