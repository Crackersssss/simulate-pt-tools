package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;

import java.sql.SQLException;

public class TableRenameHandler extends Handler {

    private static final String RENAME_SQL_HEAD = "rename table ";

    private static final String RENAME_SQL_MIDDLE = " to ";

    private static final String RENAME_OLD_TABLE_END = "_pt_old";

    public TableRenameHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
    }

    public String generateRenameSQL(final String newTableName, final String tableName) {
        return RENAME_SQL_HEAD + newTableName + RENAME_SQL_MIDDLE + tableName;
    }

    public void renameTable(final String sql) throws SQLException {
        getStatement().executeUpdate(sql);
    }

    public String getRenameOldTableName(final String tableName) {
        return tableName + RENAME_OLD_TABLE_END;
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
