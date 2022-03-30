package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.handler.TableAlterHandler;
import com.cracker.pt.onlineschemachange.handler.TableColumnsHandler;
import com.cracker.pt.onlineschemachange.handler.TableCreateHandler;
import com.cracker.pt.onlineschemachange.handler.TableDataHandler;
import com.cracker.pt.onlineschemachange.handler.TableDropHandler;
import com.cracker.pt.onlineschemachange.handler.TableRenameHandler;
import com.cracker.pt.onlineschemachange.handler.TableTriggerHandler;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;

import java.sql.SQLException;

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
        executeData(context);
        executeRename(context);
        executeDrop(context);
        if (!dataSource.getHikariDataSource().isClosed()) {
            dataSource.getHikariDataSource().close();
        }
    }

    public void executeCreate(final ExecuteContext context) {
        String newTableName = null;
        TableCreateHandler createHandler = null;
        AlterStatement alterStatement = context.getAlterStatement();
        try {
            createHandler = new TableCreateHandler(dataSource);
            createHandler.begin();
            createHandler.createTable(alterStatement);
            newTableName = createHandler.getNewTableName(alterStatement.getTableName());
            createHandler.commit();
        } catch (SQLException e) {
            try {
                if (createHandler != null) {
                    createHandler.rollback();
                }
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (createHandler != null) {
                    createHandler.close();
                }
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
        }
        context.setNewTableName(newTableName);
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
                exception.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (null != alterHandler) {
                    alterHandler.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
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
                exception.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (null != triggerHandler) {
                    triggerHandler.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void executeData(final ExecuteContext context) {
        TableDataHandler dataHandler = null;
        try {
            dataHandler = new TableDataHandler(dataSource);
            dataHandler.begin();
            String copySQL = dataHandler.generateCopySQL(context);
            dataHandler.copyData(copySQL);
            dataHandler.commit();
        } catch (SQLException e) {
            try {
                if (null != dataHandler) {
                    dataHandler.rollback();
                }
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (null != dataHandler) {
                    dataHandler.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void executeRename(final ExecuteContext context) {
        TableRenameHandler renameHandler = null;
        String renameOldTableName = null;
        AlterStatement alterStatement = context.getAlterStatement();
        try {
            renameHandler = new TableRenameHandler(dataSource);
            renameHandler.begin();
            String tableName = alterStatement.getTableName();
            renameOldTableName = renameHandler.getRenameOldTableName(tableName);
            String renameOldTableSQL = renameHandler.generateRenameSQL(tableName, renameOldTableName);
            renameHandler.renameTable(renameOldTableSQL);
            String renameNewTableSQL = renameHandler.generateRenameSQL(context.getNewTableName(), tableName);
            renameHandler.renameTable(renameNewTableSQL);
            renameHandler.commit();
        } catch (SQLException e) {
            try {
                if (null != renameHandler) {
                    renameHandler.rollback();
                }
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (null != renameHandler) {
                    renameHandler.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        context.setRenameOldTableName(renameOldTableName);
    }

    public void executeDrop(final ExecuteContext context) {
        TableDropHandler dropHandler = null;
        try {
            dropHandler = new TableDropHandler(dataSource);
            dropHandler.begin();
            String dropSQL = dropHandler.generateDropSQL(context.getRenameOldTableName());
            dropHandler.deleteTable(dropSQL);
            dropHandler.commit();
        } catch (SQLException e) {
            try {
                if (null != dropHandler) {
                    dropHandler.rollback();
                }
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (null != dropHandler) {
                    dropHandler.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
