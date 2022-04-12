package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.exception.OnlineDDLException;
import com.cracker.pt.onlineschemachange.handler.TableAlterHandler;
import com.cracker.pt.onlineschemachange.handler.TableColumnsHandler;
import com.cracker.pt.onlineschemachange.handler.TableCreateHandler;
import com.cracker.pt.onlineschemachange.handler.TableDataHandler;
import com.cracker.pt.onlineschemachange.handler.TableDropHandler;
import com.cracker.pt.onlineschemachange.handler.TableRenameHandler;
import com.cracker.pt.onlineschemachange.handler.TableResultSetHandler;
import com.cracker.pt.onlineschemachange.handler.TableSelectHandler;
import com.cracker.pt.onlineschemachange.handler.TableTriggerHandler;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

/**
 * Online DDL Executor.
 */
@Slf4j
public final class Execute {

    private final DataSource dataSource;

    public Execute(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void alterTable(final AlterStatement alterStatement) {
        ExecuteContext context = new ExecuteContext();
        context.setAlterStatement(alterStatement);
        executeCreate(context);
        executeAlter(context);
        executeTrigger(context);
        executeDataCopy(context);
        executeResultSet(context);
        executeRename(context);
        executeDrop(context);
        if (!dataSource.getHikariDataSource().isClosed()) {
            dataSource.getHikariDataSource().close();
        }
    }

    public void executeCreate(final ExecuteContext context) {
        TableCreateHandler createHandler = null;
        try {
            createHandler = new TableCreateHandler(dataSource);
            createHandler.begin();
            createHandler.getNewTableName(context);
            createHandler.createTable(context);
            createHandler.commit();
        } catch (SQLException e) {
            try {
                if (createHandler != null) {
                    createHandler.rollback();
                }
            } catch (SQLException exception) {
                throw new OnlineDDLException("An exception occurred while creating a table rollback : %s", exception.getMessage());
            }
            throw new OnlineDDLException("An exception occurred while creating the table : %s", e.getMessage());
        } finally {
            try {
                if (createHandler != null) {
                    createHandler.close();
                }
            } catch (SQLException e) {
                log.error("An error occurred while closing TableCreateHandler : {}", e.getMessage());
            }
        }
    }

    public void executeAlter(final ExecuteContext context) {
        TableAlterHandler alterHandler = null;
        try {
            alterHandler = new TableAlterHandler(dataSource);
            alterHandler.begin();
            String alterSQL = alterHandler.generateAlterSQL(context);
            alterHandler.alterTableStruct(alterSQL);
            alterHandler.commit();
        } catch (SQLException e) {
            try {
                if (null != alterHandler) {
                    alterHandler.rollback();
                }
            } catch (SQLException exception) {
                throw new OnlineDDLException("An error occurred while modifying the table rollback : %s", exception.getMessage());
            }
            throw new OnlineDDLException("An error occurred while modifying the table : %s", e.getMessage());
        } finally {
            try {
                if (null != alterHandler) {
                    alterHandler.close();
                }
            } catch (SQLException e) {
                log.error("An error occurred while closing TableAlterHandler : {}", e.getMessage());
            }
        }
    }

    public void executeTrigger(final ExecuteContext context) {
        TableColumnsHandler columnsHandler;
        TableTriggerHandler triggerHandler = null;
        try {
            columnsHandler = new TableColumnsHandler(dataSource);
            triggerHandler = new TableTriggerHandler(dataSource);
            triggerHandler.begin();
            triggerHandler.createTrigger(columnsHandler, context);
            triggerHandler.commit();
        } catch (SQLException e) {
            try {
                if (null != triggerHandler) {
                    triggerHandler.rollback();
                }
            } catch (SQLException exception) {
                throw new OnlineDDLException("An error occurred while creating the trigger rollback : %s", exception.getMessage());
            }
            throw new OnlineDDLException("An error occurred while creating the trigger : %s", e.getMessage());
        } finally {
            try {
                if (null != triggerHandler) {
                    triggerHandler.close();
                }
            } catch (SQLException e) {
                log.error("An error occurred while closing TableTriggerHandler : {}", e.getMessage());
            }
        }
    }

    public void executeDataCopy(final ExecuteContext context) {
        TableDataHandler dataHandler = null;
        TableSelectHandler selectHandler;
        try {
            selectHandler = new TableSelectHandler(dataSource);
            dataHandler = new TableDataHandler(dataSource);
            selectHandler.setCopyMinIndex(context);
            selectHandler.setCopyMaxIndex(context);
            context.setCopyStartIndex(context.getCopyMinIndex());
            selectHandler.setCopyEndIndex(context);
            context.getResultSetStartIndex().add(context.getCopyStartIndex());
            context.getResultSetEndIndex().add(context.getCopyEndIndex());
            while (true) {
                String copySQL = dataHandler.generateCopySQL(context);
                dataHandler.copyData(copySQL);
                if (context.isEnd()) {
                    break;
                }
                selectHandler.setCopyStartIndex(context);
                selectHandler.setCopyEndIndex(context);
                context.getResultSetStartIndex().add(context.getCopyStartIndex());
                context.getResultSetEndIndex().add(context.getCopyEndIndex());
            }
        } catch (SQLException e) {
            throw new OnlineDDLException("An error occurred while copying data : %s", e.getMessage());
        } finally {
            try {
                if (null != dataHandler) {
                    dataHandler.close();
                }
            } catch (SQLException e) {
                log.error("An error occurred while closing TableDataHandler : {}", e.getMessage());
            }
        }
    }

    public void executeResultSet(final ExecuteContext context) {
        TableResultSetHandler resultSetHandler = null;
        TableDropHandler dropHandler = null;
        TableTriggerHandler triggerHandler = null;
        try {
            resultSetHandler = new TableResultSetHandler(dataSource);
            resultSetHandler.begin();
            boolean comparison = resultSetHandler.resultSetComparison(context);
            if (!comparison) {
                dropHandler = new TableDropHandler(dataSource);
                String dropRecoverSQL = dropHandler.generateDropRecoverSQL(context);
                dropHandler.deleteTable(dropRecoverSQL);
                triggerHandler = new TableTriggerHandler(dataSource);
                triggerHandler.dropAllTrigger(context);
                resultSetHandler.commit();
                throw new OnlineDDLException("If the result set is inconsistent, perform the operation again!");
            }
            log.info("Consistent result set.");
            resultSetHandler.commit();
        } catch (SQLException e) {
            throw new OnlineDDLException("An error occurred while comparing result sets : %s", e.getMessage());
        } finally {
            try {
                if (null != resultSetHandler) {
                    resultSetHandler.close();
                }
                if (null != dropHandler) {
                    dropHandler.close();
                }
                if (null != triggerHandler) {
                    triggerHandler.close();
                }
            } catch (SQLException e) {
                log.error("An error occurred while closing TableResultSetHandler or TableDropHandler or TableTriggerHandler : {}", e.getMessage());
            }
        }
    }

    public void executeRename(final ExecuteContext context) {
        TableRenameHandler renameHandler = null;
        try {
            renameHandler = new TableRenameHandler(dataSource);
            renameHandler.begin();
            renameHandler.getRenameOldTableName(context);
            String renameSQL = renameHandler.generateRenameSQL(context);
            renameHandler.renameTable(renameSQL);
            renameHandler.commit();
        } catch (SQLException e) {
            try {
                if (null != renameHandler) {
                    renameHandler.rollback();
                }
            } catch (SQLException exception) {
                throw new OnlineDDLException("An error occurred during the rename table rollback : %s", exception.getMessage());
            }
            throw new OnlineDDLException("An error occurred during the rename table : %s", e.getMessage());
        } finally {
            try {
                if (null != renameHandler) {
                    renameHandler.close();
                }
            } catch (SQLException e) {
                log.error("An error occurred while closing TableRenameHandler : {}", e.getMessage());
            }
        }
    }

    public void executeDrop(final ExecuteContext context) {
        TableDropHandler dropHandler = null;
        try {
            dropHandler = new TableDropHandler(dataSource);
            dropHandler.begin();
            String dropSQL = dropHandler.generateDropSQL(context);
            dropHandler.deleteTable(dropSQL);
            dropHandler.commit();
        } catch (SQLException e) {
            try {
                if (null != dropHandler) {
                    dropHandler.rollback();
                }
            } catch (SQLException exception) {
                throw new OnlineDDLException("An error occurred while deleting a table for rollback : %s", exception.getMessage());
            }
            throw new OnlineDDLException("An error occurred while deleting a table : %s", e.getMessage());
        } finally {
            try {
                if (null != dropHandler) {
                    dropHandler.close();
                }
            } catch (SQLException e) {
                log.error("An error occurred while closing TableDropHandler : {}", e.getMessage());
            }
        }
    }
}
