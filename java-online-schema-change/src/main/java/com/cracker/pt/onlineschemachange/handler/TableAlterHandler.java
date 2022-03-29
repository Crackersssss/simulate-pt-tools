package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import com.cracker.pt.onlineschemachange.utils.AlterType;

import java.sql.SQLException;

public class TableAlterHandler extends Handler {

    private static final String ALTER_SQL_HEAD = "alter table ";

    public TableAlterHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateAlterSQL(final AlterStatement alterStatement, final String newTableName) {
        String alterSQL;
        String alterType = alterStatement.getAlterType();
        switch (AlterType.valueOf(alterType.toUpperCase())) {
            case ADD:
                alterSQL = ALTER_SQL_HEAD + newTableName + SPACE + alterType + SPACE + alterStatement.getColumnName() + SPACE + alterStatement.getColumnType() + END;
                break;
            case CHANGE:
                alterSQL = ALTER_SQL_HEAD + newTableName + SPACE + alterType + SPACE + alterStatement.getColumnName() + SPACE + alterStatement.getNewColumnName() + SPACE
                        + alterStatement.getColumnType() + END;
                break;
            case DROP:
                alterSQL = ALTER_SQL_HEAD + newTableName + SPACE + alterType + SPACE + alterStatement.getColumnName() + END;
                break;
            default:
                throw new RuntimeException("Operation " + alterType + " is not supported!");
        }
        return alterSQL;
    }

    public void alterTableStruct(final String alterStatement) throws SQLException {
        getStatement().executeUpdate(alterStatement);
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
