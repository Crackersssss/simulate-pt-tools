package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TableCreateHandler extends Handler {

    private static final String BACK_QUOTE = "`";

    private static final String RENAME_NEW_TABLE_END = "_pt_new";

    private static final String CREATE_TABLE_SQL_COLUMNS_NAME = "Create Table";

    public TableCreateHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void createTable(final AlterStatement alterStatement) throws SQLException {
        String createSQL = generateCreateTableSQL(alterStatement.getTableName());
        getStatement().executeUpdate(createSQL);
    }

    public String getNewTableName(final String tableName) {
        return tableName + RENAME_NEW_TABLE_END;
    }

    public String generateCreateTableSQL(final String tableName) throws SQLException {
        ResultSet resultSet = showOldCreateTable(tableName);
        String oldCreateTableSQL = getOldCreateTableSQL(resultSet);
        String substring = oldCreateTableSQL.substring(oldCreateTableSQL.indexOf(BACK_QUOTE), oldCreateTableSQL.indexOf(BACK_QUOTE, oldCreateTableSQL.indexOf(BACK_QUOTE) + 1) + 1);
        return oldCreateTableSQL.replace(substring, BACK_QUOTE + getNewTableName(tableName) + BACK_QUOTE);
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
