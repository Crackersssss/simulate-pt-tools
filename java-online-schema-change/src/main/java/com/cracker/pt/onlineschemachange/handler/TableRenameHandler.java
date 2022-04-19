package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;

import java.sql.SQLException;

/**
 * Table rename operating handler.
 */
public class TableRenameHandler extends Handler {

    private static final String RENAME_OLD_TABLE_END = "_pt_old";

    public TableRenameHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
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
}
