package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import lombok.Getter;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TableCreateHandler extends Handler {

    private static final String SHOW_CREATE_TABLE_HEAD = "show create table ";

    @Getter
    private String newTableName;

    private ResultSet resultSet;

    public TableCreateHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public void createTable(final AlterStatement alterStatement) throws SQLException {
        String createSQL = generateCreateTableSQL(alterStatement.getTableName());
        statement.executeUpdate(createSQL);
        resultSet.close();
        close();
    }

    public String generateCreateTableSQL(final String tableName) throws SQLException {
        showOldCreateTable(tableName);
        String oldCreateTableSQL = getOldCreateTableSQL();
        String substring = oldCreateTableSQL.substring(oldCreateTableSQL.indexOf('`'), oldCreateTableSQL.indexOf('`', oldCreateTableSQL.indexOf('`') + 1) + 1);
        newTableName = "`" + tableName + "_pt_new`";
        return oldCreateTableSQL.replace(substring, newTableName);
    }

    public ResultSet showOldCreateTable(final String tableName) throws SQLException {
        String sql = SHOW_CREATE_TABLE_HEAD + tableName + END;
        resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    public String getOldCreateTableSQL() throws SQLException {
        String result = null;
        if (resultSet != null) {
            while (resultSet.next()) {
                result = (String) resultSet.getObject("Create Table");
            }
        }
        return result;
    }
}
