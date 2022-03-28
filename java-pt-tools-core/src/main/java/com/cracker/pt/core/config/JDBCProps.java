package com.cracker.pt.core.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum JDBCProps {

    JDBCURL("jdbcUrl", "", String.class),
    USERNAME("username", "root", String.class),
    PASSWORD("password", "root$123", String.class);

    private final String propName;

    private final String defaultValue;

    private final Class<?> type;
}
