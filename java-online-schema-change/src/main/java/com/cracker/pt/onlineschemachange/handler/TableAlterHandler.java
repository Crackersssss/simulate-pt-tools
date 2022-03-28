package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.utils.AlterType;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TableAlterHandler {

    public static String generateAlterStatement(final String newTableName, final String alterType, final String oldColumnName,
                                                final String oldColumnType, final String newColumnName, final String newColumnType) {
        String alterStatement;
        switch (AlterType.valueOf(alterType.toUpperCase())) {
            case ADD:
                alterStatement = "alter table " + newTableName + " " + alterType + " " + oldColumnName + " " + oldColumnType + ";";
                break;
            case CHANGE:
                alterStatement = "alter table " + newTableName + " " + alterType + " " + oldColumnName + " " + newColumnName + " " + newColumnType;
                break;
            case DROP:
                alterStatement = "alter table " + newTableName + " " + alterType + " " + oldColumnName;
                break;
            default:
                throw new RuntimeException("Operation " + alterType + " is not supported");
        }
        return alterStatement;
    }

    public static void alterTableStruct(final DataSource dataSource, final String alterStatement) {
        try {
            HikariDataSource hikariDataSource = dataSource.getHikariDataSource();
            Connection connection = hikariDataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(alterStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
