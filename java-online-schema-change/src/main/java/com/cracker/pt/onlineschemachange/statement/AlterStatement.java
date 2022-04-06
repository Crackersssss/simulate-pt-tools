package com.cracker.pt.onlineschemachange.statement;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Alter statement.
 */
@RequiredArgsConstructor
@Getter
public class AlterStatement {

    private String tableName;

    private String alterType;

    private String columnName;

    private String columnType;

    private String newColumnName;

    public AlterStatement(final String tableName, final String alterType, final String columnName) {
        this.tableName = tableName;
        this.alterType = alterType;
        this.columnName = columnName;
    }

    public AlterStatement(final String tableName, final String alterType, final String columnName, final String columnType) {
        this(tableName, alterType, columnName);
        this.columnType = columnType;
    }

    public AlterStatement(final String tableName, final String alterType, final String oldColumnName, final String newColumnName, final String newColumnType) {
        this(tableName, alterType, oldColumnName, newColumnType);
        this.newColumnName = newColumnName;
    }
}
