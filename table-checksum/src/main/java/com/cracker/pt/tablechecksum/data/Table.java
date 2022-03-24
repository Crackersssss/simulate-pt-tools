package com.cracker.pt.tablechecksum.data;

import lombok.Getter;

@Getter
public final class Table {

    private static final java.lang.String SEPARATOR = ":";

    private final java.lang.String tableName;

    public Table(final java.lang.String tableName) {
        this.tableName = tableName.split(SEPARATOR)[1];
    }

    @Override
    public java.lang.String toString() {
        return tableName;
    }
}
