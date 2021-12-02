package com.cookpad.prism.record;

import lombok.Getter;

public class SizedValueType implements ValueType {
    @Getter
    final private ValueKind valueKind;
    @Getter
    final private int size;

    @Override
    public String toRedshiftTypeName() {
        return String.format("%s(%d)", this.valueKind.getRedshiftTypeName(), this.getSize());
    }

    public SizedValueType(ValueKind valueKind, int size) {
        if (!valueKind.isSized()) {
            throw new IllegalArgumentException("Given value kind is not sized");
        }
        this.valueKind = valueKind;
        this.size = size;
    }
}
