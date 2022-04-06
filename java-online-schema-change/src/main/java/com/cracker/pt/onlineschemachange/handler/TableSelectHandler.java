package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.utils.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Table select operating handler.
 */
public class TableSelectHandler extends Handler {

    public TableSelectHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String selectIndex(final String selectSQL, final ExecuteContext context, final String endIndex) throws SQLException {
        ResultSet resultSet = getStatement().executeQuery(selectSQL);
        String index = null;
        while (resultSet.next()) {
            index = resultSet.getString(context.getPrimaryKey());
        }
        return Optional.ofNullable(index).orElse(endIndex);
    }

    public void setCopyMinIndex(final ExecuteContext context) throws SQLException {
        String primaryKey = context.getPrimaryKey();
        String tableName = context.getAlterStatement().getTableName();
        String sql = String.format("SELECT %s FROM %s ORDER BY %s ASC LIMIT 1", primaryKey, tableName, primaryKey);
        context.setCopyMinIndex(selectIndex(sql, context, "0"));
    }

    public void setCopyMaxIndex(final ExecuteContext context) throws SQLException {
        String primaryKey = context.getPrimaryKey();
        String tableName = context.getAlterStatement().getTableName();
        String sql = String.format("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", primaryKey, tableName, primaryKey);
        context.setCopyMaxIndex(selectIndex(sql, context, "0"));
    }

    public void setCopyStartIndex(final ExecuteContext context) throws SQLException {
        String primaryKey = context.getPrimaryKey();
        String tableName = context.getAlterStatement().getTableName();
        String endIndex = context.getCopyEndIndex();
        String sql = String.format("select %s from %s where %s > %s order by %s asc limit 1;",
                primaryKey, tableName, primaryKey, endIndex, primaryKey);
        context.setCopyStartIndex(selectIndex(sql, context, endIndex));
    }

    public void setCopyEndIndex(final ExecuteContext context) throws SQLException {
        String primaryKey = context.getPrimaryKey();
        String tableName = context.getAlterStatement().getTableName();
        String copyStartIndex = context.getCopyStartIndex();
        String copyMaxIndex = context.getCopyMaxIndex();
        String copyBlockSize = "1000";
        String sql = String.format("SELECT %s FROM "
                + "(SELECT %s FROM %s WHERE (((%s >= %s))) AND (((%s <= %s))) ORDER BY %s ASC LIMIT %s) SEL1 "
                + "ORDER BY %s DESC LIMIT 1",
                primaryKey, primaryKey, tableName, primaryKey, copyStartIndex, primaryKey, copyMaxIndex, primaryKey, copyBlockSize, primaryKey);
        context.setCopyEndIndex(selectIndex(sql, context, "0"));
        if (StringUtils.compareTo(copyStartIndex, context.getCopyEndIndex()) >= 0) {
            context.setEnd(Boolean.TRUE);
        }
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
