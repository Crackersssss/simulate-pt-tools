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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;

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
        dropTrigger(context);
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

    private void dropTrigger(ExecuteContext context) {

        try {
            Statement statement = new TableSelectHandler(dataSource).getConnection().createStatement();

            String tableName = context.getAlterStatement().getTableName();
            String upTriggerName = String.format("trigger_%s_upd",tableName);
            String delTriggerName = String.format("trigger_%s_del",tableName);
            String insTriggerName = String.format("trigger_%s_ins",tableName);

            String DropTriggerSql = String.format("DROP TRIGGER IF EXISTS %s",upTriggerName);
            statement.execute(DropTriggerSql);
            DropTriggerSql = String.format("DROP TRIGGER IF EXISTS %s",delTriggerName);
            statement.execute(DropTriggerSql);
            DropTriggerSql = String.format("DROP TRIGGER IF EXISTS %s",insTriggerName);
            statement.execute(DropTriggerSql);

        } catch (SQLException e) {
            throw new RuntimeException("delete trigger fail");
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

    public static final String MAX_PKS = "maxPk";
    public static final String MIN_PKS = "minPk";

    public void executeDataCopy(final ExecuteContext context) {
        TableDataHandler dataHandler = null;
        TableSelectHandler selectHandler;

        try {
            selectHandler = new TableSelectHandler(dataSource);
            dataHandler = new TableDataHandler(dataSource);
            selectHandler.setCopyMinIndex(context);
            selectHandler.setCopyMaxIndex(context);
            context.setCopyStartIndex(context.getCopyMinIndex());
            List<String> primaryKey = context.getPrimaryKey();
            StringJoiner primaryKeySJ = new StringJoiner(",");
            for (String s : primaryKey) {
                primaryKeySJ.add(s);
            }
            List<String> copyMaxIndex = context.getCopyMaxIndex();
            List<String> copyMinIndex = context.getCopyMinIndex();
            Map<String, List<String>> primaryKeyScopList = new HashMap<>();
            primaryKeyScopList.put(MAX_PKS, copyMaxIndex);
            primaryKeyScopList.put(MIN_PKS, copyMinIndex);
            List<String> oldColumns = context.getOldColumns();
            List<String> newColumns = context.getNewColumns();
            String shardowTableName = context.getNewTableName();
            String tableName = context.getAlterStatement().getTableName();
            String database = dataSource.getDatabaseName();

            selectHandler.cutToExe(primaryKeySJ.toString(), primaryKeyScopList, newColumns, oldColumns, shardowTableName,
                    tableName, database);
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
