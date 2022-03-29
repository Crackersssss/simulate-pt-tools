package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableColumnsHandler extends Handler {

    private static final String SHOW_COLUMNS_HEAD = "SHOW COLUMNS FROM ";

    private static final String FIELD_COLUMN_NAME = "Field";

    public TableColumnsHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public List<String> getAllColumns(final String tableName) throws SQLException {
        List<String> resultSets = new ArrayList<>();
        ResultSet resultSet = getStatement().executeQuery(SHOW_COLUMNS_HEAD + tableName + END);
        while (resultSet.next()) {
            resultSets.add(String.valueOf(resultSet.getString(FIELD_COLUMN_NAME)));
        }
        return resultSets;
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
