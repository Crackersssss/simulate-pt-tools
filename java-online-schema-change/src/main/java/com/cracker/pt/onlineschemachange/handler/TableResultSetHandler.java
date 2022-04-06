package com.cracker.pt.onlineschemachange.handler;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.context.ExecuteContext;
import lombok.extern.slf4j.Slf4j;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Table result set operating handler.
 */
@Slf4j
public class TableResultSetHandler extends Handler {

    private static final String MD5 = "MD5";

    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public TableResultSetHandler(final DataSource dataSource) throws SQLException {
        super(dataSource);
        init();
    }

    public boolean resultSetComparison(final ExecuteContext context) throws SQLException {
        String oldTableName = context.getAlterStatement().getTableName();
        String newTableName = context.getNewTableName();
        List<String> oldColumns = context.getOldColumns();
        List<String> newColumns = context.getNewColumns();
        List<String> oldTableResultSet = getResultSet(oldTableName, oldColumns);
        List<String> newTableResultSet = getResultSet(newTableName, newColumns);
        return isEqual(oldTableResultSet, newTableResultSet);
    }

    private List<String> getResultSet(final String tableName, final List<String> columns) throws SQLException {
        String columnNames = columns.stream().reduce((a, b) -> a + ", " + b).orElseThrow(() -> new RuntimeException("unknown error"));
        String sql = String.format("select %s from %s;", columnNames, tableName);
        ResultSet resultSet = getStatement().executeQuery(sql);
        return processingData(resultSet);
    }

    private List<String> processingData(final ResultSet resultSet) {
        List<String> result = new ArrayList<>();
        if (resultSet != null) {
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    StringBuilder resultBuilder = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        Object values = resultSet.getObject(i);
                        resultBuilder.append(values);
                    }
                    result.add(String.valueOf(resultBuilder));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    private Optional<String> computeMD5(final String data) {
        if (null == data || data.length() == 0) {
            return Optional.empty();
        }
        try {
            MessageDigest md5 = MessageDigest.getInstance(MD5);
            md5.update(data.getBytes());
            byte[] digest = md5.digest();
            char[] chars = new char[digest.length << 1];
            int index = 0;
            for (byte b : digest) {
                chars[index++] = HEX_DIGITS[b >>> 4 & 0xf];
                chars[index++] = HEX_DIGITS[b & 0xf];
            }
            return Optional.of(new String(chars));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    private boolean isEqual(final List<String> oldTableResultSet, final List<String> newTableResultSet) {
        if (oldTableResultSet.size() != newTableResultSet.size()) {
            log.error("size inconsistency!");
            return false;
        }
        List<String> computeResult = new ArrayList<>();
        StringBuilder oldResult = oldTableResultSet.stream()
                .reduce(new StringBuilder(), (a, b) -> a.append(computeMD5(b)), (a, b) -> null);
        StringBuilder newResult = newTableResultSet.stream()
                .reduce(new StringBuilder(), (a, b) -> a.append(computeMD5(b)), (a, b) -> null);
        computeResult.add(String.valueOf(oldResult));
        computeResult.add(String.valueOf(newResult));
        String opinion = computeResult.stream()
                .map(each -> computeMD5(each).orElseThrow(() -> new RuntimeException("Line MD5 calculation error!")))
                .reduce((a, b) -> String.valueOf(a.equals(b))).orElse(String.valueOf(Boolean.FALSE));
        return String.valueOf(Boolean.TRUE).equals(opinion);
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
