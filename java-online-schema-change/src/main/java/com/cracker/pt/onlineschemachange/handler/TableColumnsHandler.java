package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Table columns operating handler.
 */
public class TableColumnsHandler extends Handler {

    private static final String COLUMN_NAME = "COLUMN_NAME";

    private static final String FIELD_COLUMN_NAME = "Field";

    public TableColumnsHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void setPrimaryKeys(final ExecuteContext context) throws SQLException {
        ResultSet resultSet = getConnection().getMetaData().getPrimaryKeys(null, null, context.getNewTableName());
        List<String> primaryKeys = new ArrayList<>();
        while (resultSet.next()) {
            primaryKeys.add(resultSet.getString(COLUMN_NAME));
        }
        if (primaryKeys.isEmpty()) {
            throw new OnlineDDLException("Table %s has no primary key", context.getAlterStatement().getTableName());
        }
        context.setPrimaryKeys(primaryKeys);
    }

    public List<String> getAllColumns(final String tableName) throws SQLException {
        List<String> resultSets = new ArrayList<>();
        String databaseName = getDatabaseName();
        String sql = String.format("SHOW COLUMNS FROM %s.%s;", databaseName, tableName);
        ResultSet resultSet = getStatement().executeQuery(sql);
        while (resultSet.next()) {
            resultSets.add(String.valueOf(resultSet.getString(FIELD_COLUMN_NAME)));
        }
        return resultSets;
    }
}
