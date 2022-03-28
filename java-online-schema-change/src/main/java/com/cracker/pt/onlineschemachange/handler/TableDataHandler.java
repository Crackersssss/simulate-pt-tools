package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;

import java.sql.SQLException;
import java.util.List;

public class TableDataHandler extends Handler {

    private static final String INSERT_SQL_HEAD = "insert into ";

    private static final String SELECT_SQL_HEAD = "select ";

    private static final String FROM = "from";

    private static final String LEFT_BRACKET = "(";

    private static final String RIGHT_BRACKET = ")";

    public TableDataHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateCopySQL(final List<String> columns, final String tableName, final String newTableName) {
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        String copySQL = INSERT_SQL_HEAD + newTableName + LEFT_BRACKET + SPACE + columnNames + RIGHT_BRACKET;
        String selectSQL = SELECT_SQL_HEAD;
        selectSQL = selectSQL + columnNames;
        selectSQL = selectSQL + SPACE + FROM + SPACE + tableName;
        copySQL = copySQL + SPACE + LEFT_BRACKET + selectSQL + RIGHT_BRACKET + END;
        return copySQL;
    }

    public void copyData(final String sql) throws SQLException {
        statement.executeUpdate(sql);
        close();
    }
}
