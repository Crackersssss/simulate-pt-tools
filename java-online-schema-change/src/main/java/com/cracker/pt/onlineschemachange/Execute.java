package com.cracker.pt.onlineschemachange;

import com.cracker.pt.core.database.DataSource;
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
        String newTableName = executeCreate(alterStatement);
        executeAlter(alterStatement, newTableName);
        //executeTrigger(alterStatement.getTableName(), newTableName);
        executeData(alterStatement, newTableName);
        String renameOldTableName = executeRename(alterStatement, newTableName);
        executeDrop(renameOldTableName);
        if (!dataSource.getHikariDataSource().isClosed()) {
            dataSource.getHikariDataSource().close();
        }
    }

    public void executeTrigger(final String tableName, final String newTableName) {
        TableColumnsHandler columnsHandler;
        TableTriggerHandler triggerHandler;
        try {
            columnsHandler = new TableColumnsHandler(dataSource);
            triggerHandler = new TableTriggerHandler(dataSource);
            triggerHandler.createTrigger(tableName, newTableName, columnsHandler);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void executeDrop(final String renameOldTableName) {
        TableDropHandler dropHandler = null;
        try {
            dropHandler = new TableDropHandler(dataSource);
            dropHandler.begin();
            String dropSQL = dropHandler.generateDropSQL(renameOldTableName);
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

    public String executeRename(final AlterStatement alterStatement, final String newTableName) {
        TableRenameHandler renameHandler = null;
        String renameOldTableName = null;
        try {
            renameHandler = new TableRenameHandler(dataSource);
            renameHandler.begin();
            renameOldTableName = renameHandler.getRenameOldTableName(alterStatement.getTableName());
            String renameOldTableSQL = renameHandler.generateRenameSQL(alterStatement.getTableName(), renameOldTableName);
            renameHandler.renameTable(renameOldTableSQL);
            String renameNewTableSQL = renameHandler.generateRenameSQL(newTableName, alterStatement.getTableName());
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
        return renameOldTableName;
    }

    public void executeData(final AlterStatement alterStatement, final String newTableName) {
        TableColumnsHandler columnsHandler;
        TableDataHandler dataHandler = null;
        try {
            columnsHandler = new TableColumnsHandler(dataSource);
            dataHandler = new TableDataHandler(dataSource);
            dataHandler.begin();
            String copySQL = dataHandler.generateCopySQL(columnsHandler, alterStatement.getAlterType(), alterStatement.getTableName(), newTableName);
            dataHandler.copyData(copySQL);
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

    public void executeAlter(final AlterStatement alterStatement, final String newTableName) {
        TableAlterHandler alterHandler = null;
        try {
            alterHandler = new TableAlterHandler(dataSource);
            alterHandler.begin();
            String alterSQL = alterHandler.generateAlterSQL(alterStatement, newTableName);
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

    public String executeCreate(final AlterStatement alterStatement) {
        String newTableName = null;
        TableCreateHandler createHandler = null;
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
        return newTableName;
    }
}
