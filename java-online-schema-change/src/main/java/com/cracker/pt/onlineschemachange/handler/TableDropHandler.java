package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;

import java.sql.SQLException;

public class TableDropHandler extends Handler {

    private static final String DROP_SQL_HEAD = "drop table ";

    public TableDropHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateDropSQL(final String tableName) {
        return DROP_SQL_HEAD + tableName + END;
    }

    public void deleteTable(final String sql) throws SQLException {
        statement.executeUpdate(sql);
        close();
    }
}
