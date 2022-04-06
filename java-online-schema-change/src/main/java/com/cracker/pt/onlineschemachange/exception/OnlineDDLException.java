package com.cracker.pt.onlineschemachange.exception;

/**
 * Online DDL exception.
 */
public class OnlineDDLException extends RuntimeException {

    public OnlineDDLException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }

    public OnlineDDLException(final Exception cause) {
        super(cause);
    }
}
