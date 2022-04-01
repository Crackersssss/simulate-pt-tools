package com.cracker.pt.onlineschemachange.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringUtils {

    public static int compareTo(final String x, final String y) {
        int result;
        try {
            result = Integer.parseInt(x) - Integer.parseInt(y);
        } catch (NumberFormatException e) {
            result = x.compareTo(y);
        }
        return result;
    }
}
