package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;

import java.sql.SQLException;

public class TableRenameHandler extends Handler {

    private static final String RENAME_SQL_HEAD = "rename table ";

    private static final String RENAME_SQL_MIDDLE = " to ";

    public TableRenameHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
    }

    public String generateRenameSQL(final String newTableName, final String tableName) {
        return RENAME_SQL_HEAD + newTableName + RENAME_SQL_MIDDLE + tableName;
    }

    public void renameTable(final String sql) throws SQLException {
        statement.executeUpdate(sql);
        close();
    }
}
