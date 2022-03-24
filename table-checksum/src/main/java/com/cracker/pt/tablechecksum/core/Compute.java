package com.cracker.pt.tablechecksum.core;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class Compute {

    private static final String SEPARATOR = "\\.";

    private static final String MD5 = "MD5";

    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private Compute() {
        throw new IllegalStateException("Utility class");
    }

    public static Optional<String> computeMD5(final String data) {
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

    public static Map<String, Boolean> isEqual(final Map<String, List<String>> results) {
        Map<String, List<String>> columns = new HashMap<>();
        Map<String, Boolean> result = new HashMap<>();
        results.forEach((k, v) -> {
            String tableName = k.split(SEPARATOR)[1];
            if (columns.containsKey(tableName)) {
                List<String> computeResult = new ArrayList<>();
                if (columns.get(tableName).size() != v.size()) {
                    result.put(tableName, Boolean.FALSE);
                } else {
                    StringBuilder master = columns.get(tableName).stream()
                            .reduce(new StringBuilder(), (a, b) -> a.append(computeMD5(b)), (a, b) -> null);
                    StringBuilder slave = v.stream().reduce(new StringBuilder(), (a, b) -> a.append(computeMD5(b)), (a, b) -> null);
                    computeResult.add(String.valueOf(master));
                    computeResult.add(String.valueOf(slave));
                }
                String opinion = computeResult.stream()
                        .map(each -> computeMD5(each).orElseThrow(() -> new RuntimeException("Line MD5 calculation error!")))
                        .reduce((a, b) -> String.valueOf(a.equals(b))).orElseThrow(() -> new RuntimeException("Failed to compare MD5 values. Procedure!"));
                if (String.valueOf(Boolean.TRUE).equals(opinion)) {
                    result.put(tableName, Boolean.TRUE);
                } else {
                    result.put(tableName, Boolean.FALSE);
                }
            } else {
                columns.put(tableName, v);
            }
        });
        return result;
    }
}
