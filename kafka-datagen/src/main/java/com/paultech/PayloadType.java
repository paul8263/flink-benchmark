package com.paultech;

import java.util.Locale;

public enum PayloadType {
    UUID("uuid"),
    ONE_KB("1kb");

    private final String literal;

    PayloadType(String literal) {
        this.literal = literal;
    }

    public static PayloadType of(String payloadType) {
        if (null == payloadType) return null;
        for (PayloadType value : PayloadType.values()) {
            if (value.literal.equalsIgnoreCase(payloadType)) {
                return value;
            }
        }
        return null;
    }
}
