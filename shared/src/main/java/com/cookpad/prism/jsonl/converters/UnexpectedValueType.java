package com.cookpad.prism.jsonl.converters;

import lombok.Getter;

public class UnexpectedValueType extends RuntimeException {
    private static final long serialVersionUID = 1L;
    @Getter
    final private String expectedType;
    UnexpectedValueType(String expectedType) {
        super("not a " + expectedType);
        this.expectedType = expectedType;
    }
}
