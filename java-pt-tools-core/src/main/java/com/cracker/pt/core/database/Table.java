package com.cracker.pt.core.database;

import lombok.Getter;

@Getter
public final class Table {

    private static final String SEPARATOR = ":";

    private final String tableName;

    public Table(final String tableName) {
        this.tableName = tableName.split(SEPARATOR)[1];
    }

    @Override
    public String toString() {
        return tableName;
    }
}
