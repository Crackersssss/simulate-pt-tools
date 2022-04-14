package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Table select operating handler.
 */
public class TableSelectHandler extends Handler {

    public TableSelectHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public List<String> selectIndex(final String selectSQL, final ExecuteContext context) throws SQLException {
        ResultSet resultSet = getStatement().executeQuery(selectSQL);
        List<String> index = new ArrayList<>();
        List<String> primaryKey = context.getPrimaryKeys();
        while (resultSet.next()) {
            for (String s : primaryKey) {
                index.add(resultSet.getString(s));
            }
        }
        return index;
    }

    public void setCopyMinIndex(final ExecuteContext context) throws SQLException {
        List<String> primaryKeys = context.getPrimaryKeys();
        StringJoiner primaryKeyStr = primaryKeys.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        StringJoiner ascPk = primaryKeys.stream().reduce(new StringJoiner(" ASC, ", "", " ASC"), StringJoiner::add, (a, b) -> null);
        String tableName = context.getAlterStatement().getTableName();
        String sql = String.format("SELECT %s FROM %s ORDER BY %s LIMIT 1", primaryKeyStr, tableName, ascPk);
        context.setCopyMinIndex(selectIndex(sql, context));
    }

    public void setCopyMaxIndex(final ExecuteContext context) throws SQLException {
        List<String> primaryKeys = context.getPrimaryKeys();
        StringJoiner primaryKeyStr = primaryKeys.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        StringJoiner descPK = primaryKeys.stream().reduce(new StringJoiner(" DESC, ", "", " DESC"), StringJoiner::add, (a, b) -> null);
        String tableName = context.getAlterStatement().getTableName();
        String sql = String.format("SELECT %s FROM %s ORDER BY %s LIMIT 1", primaryKeyStr, tableName, descPK);
        context.setCopyMaxIndex(selectIndex(sql, context));
    }
}
