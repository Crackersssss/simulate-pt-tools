package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import com.cracker.pt.onlineschemachange.utils.StringUtils;

import java.sql.*;
import java.util.*;

/**
 * Table select operating handler.
 */
public class TableSelectHandler extends Handler {

    public TableSelectHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public List<String> selectIndex(final String selectSQL, final ExecuteContext context) throws SQLException {
        ResultSet resultSet = getStatement().executeQuery(selectSQL);
        List<String> index = new ArrayList<>();
        List<String> primaryKey = context.getPrimaryKey();
        while (resultSet.next()) {
            for (String s : primaryKey) {
                index.add(resultSet.getString(s));
            }
        }
        return index;
    }

    public void setCopyMinIndex(final ExecuteContext context) throws SQLException {
        List<String> primaryKey = context.getPrimaryKey();
        StringJoiner Pk = new StringJoiner(",");
        for (String s : primaryKey) {
            Pk.add(s);
        }
        StringJoiner ascPk = new StringJoiner(" ASC, ", "", " ASC");
        for (String s : primaryKey) {
            ascPk.add(s);
        }
        String tableName = context.getAlterStatement().getTableName();
        String sql = String.format("SELECT %s FROM %s ORDER BY %s LIMIT 1", Pk, tableName, ascPk);
        context.setCopyMinIndex(selectIndex(sql, context));
    }

    public void setCopyMaxIndex(final ExecuteContext context) throws SQLException {
        List<String> primaryKey = context.getPrimaryKey();
        StringJoiner Pk = new StringJoiner(",");
        for (String s : primaryKey) {
            Pk.add(s);
        }
        StringJoiner ascPk = new StringJoiner(" DESC, ", "", " DESC");
        for (String s : primaryKey) {
            ascPk.add(s);
        }
        String tableName = context.getAlterStatement().getTableName();
        String sql = String.format("SELECT %s FROM %s ORDER BY %s LIMIT 1", Pk, tableName, ascPk);
        context.setCopyMaxIndex(selectIndex(sql, context));
    }


    public static final String MAX_PKS = "maxPk";
    public static final String MIN_PKS = "minPk";

    public void cutToExe(String primaryKeyStr, Map<String, List<String>> primaryKeyScopList, List<String> newColumnList,
                         List<String> oldColumnList,
                         String shardowTableName, String tableName, String database) throws SQLException {


        String[] primaryKeys = primaryKeyStr.split(",");

        //取出范围值
        List<String> minPkList = primaryKeyScopList.get(MIN_PKS);
        List<String> maxPkList = primaryKeyScopList.get(MAX_PKS);
        //是否是第一次发送
        Map<String, Boolean> isFirstExe = new HashMap<>();
        isFirstExe.put("ISFIRST", true);
        if (maxPkList==null) {
            return;
        }
        String minColumn = minPkList.get(0);
        String maxColumn = maxPkList.get(0);

        //局部范围10000内的主键范围
        List<String> localMinPkList = new ArrayList<>(minPkList);
        List<String> localMaxPkList = new ArrayList<>(maxPkList);

        //查询切割10000条
        while (minColumn.compareTo(maxColumn) < 0) {
            //前置查询
            String searchSql = makeSearchSql(primaryKeyStr, localMinPkList, localMaxPkList, database, tableName);
            //执行查询获得之后第10001条主键值
            ResultSet resultSet = getStatement().executeQuery(searchSql);
            //取出主键值
            ArrayList<String> pkList = new ArrayList<>();
            while (resultSet.next()) {
                for (int i = 0; i < primaryKeys.length; i++) {
                    pkList.add(resultSet.getString(primaryKeys[i]));
                }
            }
            resultSet.close();
            //插入sql
            String insertSql = makeInsertSql(shardowTableName, primaryKeyStr, newColumnList,
                    oldColumnList, localMinPkList, pkList,
                    database, tableName, isFirstExe);
            getStatement().execute(insertSql);
            localMinPkList = pkList;
            minColumn = localMinPkList.get(0);
        }
    }

    public String makeInsertSql(String shardowTableName, String primaryKey, List<String> newColumnList,
                                List<String> oldColumnList,
                                List<String> localMinPkList, List<String> localMaxPkList, String database,
                                String tableName, Map<String, Boolean> isFirstExe) {

        String[] primaryKeys = primaryKey.split(",");
        //主键数
        int pkNum = primaryKeys.length;

        StringJoiner searchMoreWhereSql = new StringJoiner(") OR (", "(", ")");
        String preSearch = "";
        String[] keymin = new String[pkNum];
        String[] keymax = new String[pkNum];
        for (int i = 0; i < pkNum; i++) {
            keymin[i] = localMinPkList.get(i);
        }
        for (int i = 0; i < pkNum; i++) {
            keymax[i] = localMaxPkList.get(i);
        }
        for (int i = 0; i < pkNum; i++) {
            if (i == 0) {
                searchMoreWhereSql.add(preSearch.replace(">", "=") + primaryKeys[i] + " > '" + keymin[i] + "'");
                preSearch += primaryKeys[i] + " > '" + keymin[i] + "'";
            } else {
                searchMoreWhereSql.add(preSearch.replace(">", "=") + " AND " + primaryKeys[i] + " > '" + keymin[i] + "'");
                preSearch += " AND " + primaryKeys[i] + " > '" + keymin[i] + "'";
            }
        }


        if (isFirstExe.get("ISFIRST")) {
            isFirstExe.put("ISFIRST", false);
            searchMoreWhereSql.add(preSearch.replace(">", "="));
        }

        StringJoiner searchLessWhereSql = new StringJoiner(") OR (", "(", ")");
        String preLessSearch = "";
        for (int i = 0; i < pkNum; i++) {
            keymin[i] = localMinPkList.get(i);
        }
        for (int i = 0; i < pkNum; i++) {
            if (i == 0) {
                searchLessWhereSql.add(preLessSearch.replace("<", "=") + primaryKeys[i] + " < '" + keymax[i] + "'");
                preLessSearch += primaryKeys[i] + " < '" + keymax[i] + "'";
            } else {
                searchLessWhereSql.add(preLessSearch.replace("<", "=") + " AND " + primaryKeys[i] + " < '" + keymax[i] + "'");
                preLessSearch += " AND " + primaryKeys[i] + " < '" + keymax[i] + "'";
            }
        }
        searchLessWhereSql.add(preLessSearch.replace("<", "="));
        StringJoiner newColumnStr = new StringJoiner(",");
        for (String s : newColumnList) {
            newColumnStr.add(s);
        }
        StringJoiner oldColumnStr = new StringJoiner(",");
        for (String s : oldColumnList) {
            oldColumnStr.add(s);
        }
        String insertSql = String.format("INSERT IGNORE INTO %s.%s (%s) (SELECT %s FROM %s.%s force index (primary) " +
                        "where (%s) AND (%s))"
                , database, shardowTableName, oldColumnStr, newColumnStr, database, tableName, searchMoreWhereSql, searchLessWhereSql);

        return insertSql;
    }


    public String makeSearchSql(String primaryKey, List<String> localMinPkList, List<String> localMaxPkList, String database, String tableName) {
        String[] primaryKeys = primaryKey.split(",");
        //主键数
        int pkNum = primaryKeys.length;
        //存储前半段>的sql
        StringJoiner searchMoreWhereSql = new StringJoiner(") OR (", "(", ")");
        String preSearch = "";
        String[] keymin = new String[pkNum];
        String[] keymax = new String[pkNum];
        for (int i = 0; i < pkNum; i++) {
            keymin[i] = localMinPkList.get(i);
        }
        for (int i = 0; i < pkNum; i++) {
            keymax[i] = localMaxPkList.get(i);
        }
        for (int i = 0; i < pkNum; i++) {
            //如果是第一次不需要AND衔接
            if (i == 0) {
                searchMoreWhereSql.add(preSearch.replace(">", "=") + primaryKeys[i] + ">'" + keymin[i] + "'");
                preSearch += primaryKeys[i] + ">'" + keymin[i] + "'";
            } else {
                searchMoreWhereSql.add(preSearch.replace(">", "=") + " AND " + primaryKeys[i] + ">'" + keymin[i] + "'");
                preSearch += " AND " + primaryKeys[i] + ">'" + keymin[i] + "'";
            }
        }
        searchMoreWhereSql.add(preSearch.replace(">", "="));

        //存储后半段<的sql
        StringJoiner searchLessWhereSql = new StringJoiner(") OR (", "(", ")");
        String preLessSearch = "";

        for (int i = 0; i < pkNum; i++) {
            //如果是第一次不需要AND衔接
            if (i == 0) {
                searchLessWhereSql.add(preLessSearch.replace("<", "=") + primaryKeys[i] + "<'" + keymax[i] + "'");
                preLessSearch += primaryKeys[i] + "<'" + keymax[i] + "'";
            } else {
                searchLessWhereSql.add(preLessSearch.replace("<", "=") + " AND " + primaryKeys[i] + "<'" + keymax[i] + "'");
                preLessSearch += " AND " + primaryKeys[i] + "<'" + keymax[i] + "'";
            }
        }
        searchLessWhereSql.add(preLessSearch.replace("<", "="));

        StringJoiner Asc = new StringJoiner(" ASC, ", "", " ASC LIMIT 10001) SEL1\n");
        for (int i = 0; i < primaryKeys.length; i++) {
            Asc.add(primaryKeys[i]);
        }
        StringJoiner Desc = new StringJoiner(" DESC, ", " ORDER BY \n", " DESC LIMIT 1");
        for (int i = 0; i < primaryKeys.length; i++) {
            Desc.add(primaryKeys[i]);
        }
        return String.format("SELECT %s FROM \n" +
                        "(SELECT %s FROM %s.%s WHERE \n(%s)" + "\n AND \n" + "(%s)\n" +
                        " ORDER BY \n %s %s", primaryKey, primaryKey, database, tableName, searchMoreWhereSql,
                searchLessWhereSql,
                Asc, Desc);

        //
        //return "SELECT " + primaryKey + " FROM \n" +
        //        "(SELECT " + primaryKey + " FROM " + database + "." + tableName + " WHERE \n(" +
        //        searchMoreWhereSql + ")" + "\n AND \n" + "(" + searchLessWhereSql + ")\n" +
        //        " ORDER BY \n" + Asc + Desc;
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
