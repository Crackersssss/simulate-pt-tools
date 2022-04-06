package com.cracker.pt.tablechecksum.query;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;

public abstract class Result {

    private static final String STRING_FORMAT = "{0}:{1}";

    protected Result() {
        throw new IllegalStateException("Utility class");
    }

    protected static StringBuilder getResult(final ResultSet resultSet, final ResultSetMetaData metaData, final int columnCount) throws SQLException {
        StringBuilder resultBuilder = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            Object values = resultSet.getObject(i);
            resultBuilder.append(MessageFormat.format(STRING_FORMAT, columnName, values));
        }
        return resultBuilder;
    }
}
