package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Table create operating handler.
 */
public class TableCreateHandler extends Handler {

    private static final String BACK_QUOTE = "`";

    private static final String RENAME_NEW_TABLE_END = "_pt_new";

    private static final String CREATE_TABLE_SQL_COLUMNS_NAME = "Create Table";

    public TableCreateHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void createTable(final ExecuteContext context) throws SQLException {
        String createSQL = generateCreateTableSQL(context);
        getStatement().executeUpdate(createSQL);
    }

    public void getNewTableName(final ExecuteContext context) {
        context.setNewTableName(context.getAlterStatement().getTableName() + RENAME_NEW_TABLE_END);
    }

    public String generateCreateTableSQL(final ExecuteContext context) throws SQLException {
        ResultSet resultSet = showOldCreateTable(context.getAlterStatement().getTableName());
        String oldCreateTableSQL = getOldCreateTableSQL(resultSet);
        String substring = oldCreateTableSQL.substring(oldCreateTableSQL.indexOf(BACK_QUOTE), oldCreateTableSQL.indexOf(BACK_QUOTE, oldCreateTableSQL.indexOf(BACK_QUOTE) + 1) + 1);
        return oldCreateTableSQL.replace(substring, BACK_QUOTE + context.getNewTableName() + BACK_QUOTE);
    }

    public ResultSet showOldCreateTable(final String tableName) throws SQLException {
        String sql = String.format("show create table %s;", tableName);
        return getStatement().executeQuery(sql);
    }

    public String getOldCreateTableSQL(final ResultSet resultSet) throws SQLException {
        String result = null;
        if (resultSet != null) {
            while (resultSet.next()) {
                result = (String) resultSet.getObject(CREATE_TABLE_SQL_COLUMNS_NAME);
            }
        }
        return result;
    }
}
