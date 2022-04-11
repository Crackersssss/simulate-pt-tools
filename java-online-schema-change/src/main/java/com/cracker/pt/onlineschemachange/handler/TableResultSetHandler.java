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
        String leftSQL = getSubSQL(context.getAlterStatement().getTableName(), context.getOldColumns());
        String rightSQL = getSubSQL(context.getNewTableName(), context.getNewColumns());
        return getResult(leftSQL, rightSQL);
    }

    private boolean getResult(final String leftSQL, final String rightSQL) throws SQLException {
        String sql = String.format("%s UNION ALL %s;", leftSQL, rightSQL);
        ResultSet resultSet = getStatement().executeQuery(sql);
        List<String> results = processingData(resultSet);
        String result = results.stream().reduce((a, b) -> String.valueOf(a.equals(b))).orElseThrow(() -> new RuntimeException("unknown error"));
        return String.valueOf(Boolean.TRUE).equals(result);
    }

    private String getSubSQL(final String tableName, final List<String> columns) {
        StringBuilder columnNames = new StringBuilder();
        columns.forEach(each -> columnNames.append(String.format("ifnull(%s, NULL), ", each)));
        return String.format("SELECT sum(crc32(concat(%s))) AS sum FROM %s", columnNames.substring(0, columnNames.length() - 2), tableName);
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

    @SuppressWarnings("unused")
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
