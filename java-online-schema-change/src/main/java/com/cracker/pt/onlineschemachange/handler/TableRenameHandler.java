package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;

import java.sql.SQLException;

public class TableRenameHandler extends Handler {

    private static final String RENAME_OLD_TABLE_END = "_pt_old";

    public TableRenameHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
    }

    public String generateRenameSQL(final String newTableName, final String tableName) {
        return String.format("rename table %s to %s;", newTableName, tableName);
    }

    public String generateRenameSQL(final ExecuteContext context) {
        String tableName = context.getAlterStatement().getTableName();
        return String.format("rename table %s to %s, %s to %s;", tableName, context.getRenameOldTableName(), context.getNewTableName(), tableName);
    }

    public void renameTable(final String sql) throws SQLException {
        getStatement().executeUpdate(sql);
    }

    public void getRenameOldTableName(final ExecuteContext context) {
        String tableName = context.getAlterStatement().getTableName();
        context.setRenameOldTableName(tableName + RENAME_OLD_TABLE_END);
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
