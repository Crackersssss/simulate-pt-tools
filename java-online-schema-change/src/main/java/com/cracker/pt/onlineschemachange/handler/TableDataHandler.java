package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Table data operating handler.
 */
@Slf4j
public class TableDataHandler extends Handler {

    private boolean isFirst;

    public TableDataHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
        isFirst = false;
    }

    /**
     * Repeat function with other methods.
     * @deprecated  repeat function with other methods.
     * @param context execute context
     * @return copy sql
     */
    @Deprecated
    public String generateCopySQL(final ExecuteContext context) {
        List<String> oldColumns = context.getOldColumns();
        List<String> newColumns = context.getNewColumns();
        return getCopySQL(oldColumns, newColumns, context);
    }

    /**
     * Repeat function with other methods.
     * @deprecated  repeat function with other methods.
     */
    @Deprecated
    private String getCopySQL(final List<String> oldColumns, final List<String> newColumns, final ExecuteContext context) {
        String selectSQL = getSelectSQL(oldColumns, context);
        return getSubCopySQL(newColumns, selectSQL, context);
    }

    /**
     * Repeat function with other methods.
     * @deprecated  repeat function with other methods.
     */
    @Deprecated
    private String getSelectSQL(final List<String> columns, final ExecuteContext context) {
        String tableName = context.getAlterStatement().getTableName();
        List<String> primaryKey = context.getPrimaryKeys();
        List<String> copyStartIndex = context.getCopyStartIndex();
        List<String> copyEndIndex = context.getCopyEndIndex();
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        return String.format("select %s from %s where %s >= '%s' and %s <= '%s'",
                columnNames, tableName, primaryKey, copyStartIndex, primaryKey, copyEndIndex);
    }

    /**
     * Repeat function with other methods.
     * @deprecated  repeat function with other methods.
     */
    @Deprecated
    private String getSubCopySQL(final List<String> columns, final String selectSQL, final ExecuteContext context) {
        String newTableName = context.getNewTableName();
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        return String.format("REPLACE into %s(%s) (%s);", newTableName, columnNames, selectSQL);
    }

    public void copyData(final ExecuteContext context, final String database) throws SQLException {
        List<String> primaryKey = context.getPrimaryKeys();
        StringJoiner primaryKeySJ = primaryKey.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        String primaryKeyStr = primaryKeySJ.toString();
        String[] primaryKeys = primaryKeyStr.split(", ");
        //???????????????
        List<String> minPkList = context.getCopyMinIndex();
        List<String> maxPkList = context.getCopyMaxIndex();
        //????????????????????????
        isFirst = true;
        if (maxPkList == null) {
            return;
        }
        //????????????10000??????????????????
        List<String> localMinPkList = new ArrayList<>(minPkList);
        List<String> localMaxPkList = new ArrayList<>(maxPkList);
        //????????????????????????????????????????????????????????????while???????????????????????????????????????
        String tableName = context.getAlterStatement().getTableName();
        Integer countTable = countTableLine(database, tableName);
        List<String> oldColumns = context.getOldColumns();
        StringJoiner oldColumnSJ = oldColumns.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        List<String> newColumns = context.getNewColumns();
        StringJoiner newColumnSJ = newColumns.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        String newTableName = context.getNewTableName();
        if (countTable == 1) {
            String sql = String.format("INSERT INTO %s(%s) (SELECT %s FROM %s)", newTableName, newColumnSJ,
                    oldColumnSJ, tableName);
            getStatement().execute(sql);
            return;
        }
        //????????????10000???
        while (computeEnd(localMinPkList, localMaxPkList) < 0) {
            //????????????
            String searchSql = makeSearchSql(primaryKeyStr, localMinPkList, localMaxPkList, database, tableName);
            //???????????????????????????10001????????????
            ResultSet resultSet = getStatement().executeQuery(searchSql);
            //???????????????
            ArrayList<String> pkList = new ArrayList<>();
            while (resultSet.next()) {
                for (String key : primaryKeys) {
                    pkList.add(resultSet.getString(key));
                }
            }
            if (pkList.isEmpty()) {
                return;
            }
                    //??????sql
            String insertSql = makeInsertSql(newTableName, primaryKeys, newColumns,
                    oldColumns, localMinPkList, pkList,
                    database, tableName);
            getStatement().execute(insertSql);
            if (String.join("", localMinPkList).equals(String.join("", pkList))) {
                return;
            }
            localMinPkList = pkList;
        }
    }

    private int computeEnd(final List<String> localMinPkList, final List<String> localMaxPkList) {
        int compare = 0;
        for (int i = 0; i < localMinPkList.size(); i++) {
            compare = StringUtils.compareTo(localMinPkList.get(i), localMaxPkList.get(i));
            if (compare != 0) {
                return compare;
            }
        }
        return compare;
    }

    private Integer countTableLine(final String database, final String tableName) throws SQLException {
        String sql = String.format("SELECT COUNT(*) AS COUNT FROM %s.%s", database, tableName);
        ResultSet resultSet = getStatement().executeQuery(sql);
        return resultSet.next() ? resultSet.getInt("COUNT") : -1;
    }

    public String makeInsertSql(final String newTableName, final String[] primaryKeys, final List<String> newColumnList,
                                final List<String> oldColumnList,
                                final List<String> localMinPkList, final List<String> pKList, final String database,
                                final String tableName) {
        //?????????
        int pkNum = primaryKeys.length;
        StringJoiner searchMoreWhereSql = new StringJoiner(") OR (", "(", ")");
        StringBuilder preSearch = new StringBuilder();
        for (int i = 0; i < pkNum; i++) {
            if (i == 0) {
                searchMoreWhereSql.add(preSearch.toString().replace(">", "=") + primaryKeys[i] + " > '" + localMinPkList.get(i) + "'");
                preSearch.append(primaryKeys[i]).append(" > '").append(localMinPkList.get(i)).append("'");
            } else {
                searchMoreWhereSql.add(preSearch.toString().replace(">", "=") + " AND " + primaryKeys[i] + " > '" + localMinPkList.get(i) + "'");
                preSearch.append(" AND ").append(primaryKeys[i]).append(" > '").append(localMinPkList.get(i)).append("'");
            }
        }
        if (isFirst) {
            isFirst = false;
            searchMoreWhereSql.add(preSearch.toString().replace(">", "="));
        }
        StringJoiner searchLessWhereSql = new StringJoiner(") OR (", "(", ")");
        StringBuilder preLessSearch = new StringBuilder();
        for (int i = 0; i < pkNum; i++) {
            if (i == 0) {
                searchLessWhereSql.add(preLessSearch.toString().replace("<", "=") + primaryKeys[i] + " < '" + pKList.get(i) + "'");
                preLessSearch.append(primaryKeys[i]).append(" < '").append(pKList.get(i)).append("'");
            } else {
                searchLessWhereSql.add(preLessSearch.toString().replace("<", "=") + " AND " + primaryKeys[i] + " < '" + pKList.get(i) + "'");
                preLessSearch.append(" AND ").append(primaryKeys[i]).append(" < '").append(pKList.get(i)).append("'");
            }
        }
        searchLessWhereSql.add(preLessSearch.toString().replace("<", "="));
        StringJoiner newColumnStr = newColumnList.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        StringJoiner oldColumnStr = oldColumnList.stream().reduce(new StringJoiner(", "), StringJoiner::add, (a, b) -> null);
        return String.format("INSERT IGNORE INTO %s.%s (%s) (SELECT %s FROM %s.%s force index (primary) where (%s) AND (%s))",
                database, newTableName, newColumnStr, oldColumnStr, database, tableName, searchMoreWhereSql, searchLessWhereSql);
    }

    public String makeSearchSql(final String primaryKey, final List<String> localMinPkList, final List<String> localMaxPkList, final String database, final String tableName) {
        String[] primaryKeys = primaryKey.split(",");
        //?????????
        int pkNum = primaryKeys.length;
        //???????????????>???sql
        StringJoiner searchMoreWhereSql = new StringJoiner(") OR (", "(", ")");
        StringBuilder preSearch = new StringBuilder();
        for (int i = 0; i < pkNum; i++) {
            //???????????????????????????AND??????
            if (i == 0) {
                searchMoreWhereSql.add(preSearch.toString().replace(">", "=") + primaryKeys[i] + ">'" + localMinPkList.get(i) + "'");
                preSearch.append(primaryKeys[i]).append(">'").append(localMinPkList.get(i)).append("'");
            } else {
                searchMoreWhereSql.add(preSearch.toString().replace(">", "=") + " AND " + primaryKeys[i] + ">'" + localMinPkList.get(i) + "'");
                preSearch.append(" AND ").append(primaryKeys[i]).append(">'").append(localMinPkList.get(i)).append("'");
            }
        }
        searchMoreWhereSql.add(preSearch.toString().replace(">", "="));

        //???????????????<???sql
        StringJoiner searchLessWhereSql = new StringJoiner(") OR (", "(", ")");
        StringBuilder preLessSearch = new StringBuilder();

        for (int i = 0; i < pkNum; i++) {
            //???????????????????????????AND??????
            if (i == 0) {
                searchLessWhereSql.add(preLessSearch.toString().replace("<", "=") + primaryKeys[i] + "<'" + localMaxPkList.get(i) + "'");
                preLessSearch.append(primaryKeys[i]).append("<'").append(localMaxPkList.get(i)).append("'");
            } else {
                searchLessWhereSql.add(preLessSearch.toString().replace("<", "=") + " AND " + primaryKeys[i] + "<'" + localMaxPkList.get(i) + "'");
                preLessSearch.append(" AND ").append(primaryKeys[i]).append("<'").append(localMaxPkList.get(i)).append("'");
            }
        }
        searchLessWhereSql.add(preLessSearch.toString().replace("<", "="));
        StringJoiner asc = new StringJoiner(" ASC, ", "", " ASC LIMIT 10001) SEL1");
        for (String s : primaryKeys) {
            asc.add(s);
        }
        StringJoiner desc = new StringJoiner(" DESC, ", " ORDER BY ", " DESC LIMIT 1");
        for (String key : primaryKeys) {
            desc.add(key);
        }
        return String.format("SELECT %s FROM (SELECT %s FROM %s.%s WHERE (%s) AND (%s) ORDER BY %s %s",
                primaryKey, primaryKey, database, tableName, searchMoreWhereSql, searchLessWhereSql, asc, desc);
    }
}
