package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.context.ExecuteDatasource;
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.sql.SQLException;
import java.util.List;

/**
 * Online DDL Executor.
 */
@Slf4j
public final class Execute {

    private final List<ExecuteDatasource> executeDatasourceList;

    public Execute(final List<ExecuteDatasource> executeDatasourceList) {
        this.executeDatasourceList = executeDatasourceList;
    }

    public void alterTable() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(executeDatasourceList.size());
        taskExecutor.setMaxPoolSize(executeDatasourceList.size() + 1);
        taskExecutor.setQueueCapacity(Integer.MAX_VALUE);
        taskExecutor.setKeepAliveSeconds(60);
        taskExecutor.setThreadNamePrefix("OnlineDDLStartThread");
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.initialize();
        executeDatasourceList.forEach(each -> taskExecutor.submitListenable(() -> {
            ExecuteContext context = new ExecuteContext();
            each.setContext(context);
            context.setAlterStatement(each.getAlterStatement());
            dropTrigger(each);
            executeCreate(each);
            executeAlter(each);
            executeTrigger(each);
            executeDataCopy(each);
            executeResultSet(each);
        }).addCallback(data -> log.info("success,result = {}", data), ex -> log.info("**exception message**：{}, {}", ex.getMessage(), ex.getCause())));
        taskExecutor.shutdown();
        while (taskExecutor.getActiveCount() > 0) {
            //log.info("Modify, please wait for......");
        }
        executeDatasourceList.forEach(each -> {
            if (!each.getContext().isSuccess()) {
                if (!each.getDataSource().getHikariDataSource().isClosed()) {
                    each.getDataSource().getHikariDataSource().close();
                }
                throw new OnlineDDLException("An error occurred while modifying the table structure and execution was stopped");
            }
        });
        executeDatasourceList.forEach(each -> {
            executeRename(each);
            executeDrop(each);
        });
    }

    private void dropTrigger(final ExecuteDatasource executeDatasource) {
        DataSource dataSource = executeDatasource.getDataSource();
        ExecuteContext context = executeDatasource.getContext();
        try (TableTriggerHandler triggerHandler = new TableTriggerHandler(dataSource)) {
            String tableName = context.getAlterStatement().getTableName();
            triggerHandler.dropTrigger(String.format("trigger_%s_upd", tableName));
            triggerHandler.dropTrigger(String.format("trigger_%s_del", tableName));
            triggerHandler.dropTrigger(String.format("trigger_%s_ins", tableName));
        } catch (SQLException e) {
            throw new OnlineDDLException("Delete trigger failed on initialization : %s", e.getMessage());
        }
    }

    public void executeCreate(final ExecuteDatasource executeDatasource) {
        TableCreateHandler createHandler = null;
        try {
            createHandler = new TableCreateHandler(executeDatasource.getDataSource());
            createHandler.begin();
            createHandler.getNewTableName(executeDatasource.getContext());
            createHandler.createTable(executeDatasource.getContext());
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

    public void executeAlter(final ExecuteDatasource executeDatasource) {
        TableAlterHandler alterHandler = null;
        try {
            alterHandler = new TableAlterHandler(executeDatasource.getDataSource());
            alterHandler.begin();
            String alterSQL = alterHandler.generateAlterSQL(executeDatasource.getContext());
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

    public void executeTrigger(final ExecuteDatasource executeDatasource) {
        TableColumnsHandler columnsHandler;
        TableTriggerHandler triggerHandler = null;
        try {
            columnsHandler = new TableColumnsHandler(executeDatasource.getDataSource());
            triggerHandler = new TableTriggerHandler(executeDatasource.getDataSource());
            triggerHandler.begin();
            triggerHandler.createTrigger(columnsHandler, executeDatasource.getContext());
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

    public void executeDataCopy(final ExecuteDatasource executeDatasource) {
        DataSource dataSource = executeDatasource.getDataSource();
        try (TableDataHandler dataHandler = new TableDataHandler(dataSource);
             TableSelectHandler selectHandler = new TableSelectHandler(dataSource)) {
            ExecuteContext context = executeDatasource.getContext();
            selectHandler.setCopyMinIndex(context);
            selectHandler.setCopyMaxIndex(context);
            context.setCopyStartIndex(context.getCopyMinIndex());
            dataHandler.copyData(context, dataSource.getDatabaseName());
        } catch (SQLException e) {
            throw new OnlineDDLException("An error occurred while copying data : %s", e.getMessage());
        }
    }

    public void executeResultSet(final ExecuteDatasource executeDatasource) {
        DataSource dataSource = executeDatasource.getDataSource();
        try (TableResultSetHandler resultSetHandler = new TableResultSetHandler(dataSource);
             TableDropHandler dropHandler = new TableDropHandler(dataSource);
             TableTriggerHandler triggerHandler = new TableTriggerHandler(dataSource);) {
            resultSetHandler.begin();
            ExecuteContext context = executeDatasource.getContext();
            boolean comparison = resultSetHandler.resultSetComparison(context);
            if (!comparison) {
                String dropRecoverSQL = dropHandler.generateDropRecoverSQL(context);
                dropHandler.deleteTable(dropRecoverSQL);
                triggerHandler.dropAllTrigger(context);
                resultSetHandler.commit();
                throw new OnlineDDLException("If the result set is inconsistent, perform the operation again!");
            }
            log.info("Consistent result set.");
            context.setSuccess(true);
            resultSetHandler.commit();
        } catch (SQLException e) {
            throw new OnlineDDLException("An error occurred while comparing result sets : %s", e.getMessage());
        }
    }

    public void executeRename(final ExecuteDatasource executeDatasource) {
        TableRenameHandler renameHandler = null;
        try {
            renameHandler = new TableRenameHandler(executeDatasource.getDataSource());
            renameHandler.begin();
            ExecuteContext context = executeDatasource.getContext();
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

    public void executeDrop(final ExecuteDatasource executeDatasource) {
        TableDropHandler dropHandler = null;
        try {
            dropHandler = new TableDropHandler(executeDatasource.getDataSource());
            dropHandler.begin();
            String dropSQL = dropHandler.generateDropSQL(executeDatasource.getContext());
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
