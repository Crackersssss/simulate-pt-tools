package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import com.cracker.pt.onlineschemachange.utils.AlterType;

import java.sql.SQLException;

/**
 * Table alter operating handler.
 */
public class TableAlterHandler extends Handler {

    public TableAlterHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public String generateAlterSQL(final ExecuteContext context) {
        String alterSQL;
        AlterStatement alterStatement = context.getAlterStatement();
        String alterType = alterStatement.getAlterType();
        String newTableName = context.getNewTableName();
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

    public void alterTableStruct(final String alterSQL) throws SQLException {
        getStatement().executeUpdate(alterSQL);
    }
}
