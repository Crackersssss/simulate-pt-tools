package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import com.cracker.pt.onlineschemachange.utils.AlterType;

import java.sql.SQLException;

public class TableAlterHandler extends Handler {

    public TableAlterHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateAlterSQL(final AlterStatement alterStatement, final String newTableName) {
        String alterSQL;
        String alterType = alterStatement.getAlterType();
        switch (AlterType.valueOf(alterType.toUpperCase())) {
            case ADD:
                alterSQL = String.format("alter table %s %s %s %s;", newTableName, alterType, alterStatement.getColumnName(), alterStatement.getColumnType());
                break;
            case CHANGE:
                alterSQL = String.format("alter table %s %s %s %s %s;", newTableName, alterType, alterStatement.getColumnName(), alterStatement.getNewColumnName(), alterStatement.getColumnType());
                break;
            case DROP:
                alterSQL = String.format("alter table %s %s %s;", newTableName, alterType, alterStatement.getColumnName());
                break;
            default:
                throw new OnlineDDLException("Operation %s is not supported!", alterType);
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
