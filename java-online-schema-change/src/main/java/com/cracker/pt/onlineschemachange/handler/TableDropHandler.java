package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;

import java.sql.SQLException;

public class TableDropHandler extends Handler {

    public TableDropHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateDropSQL(final String tableName) {
        return String.format("drop table %s;", tableName);
    }

    public void deleteTable(final String sql) throws SQLException {
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
