package com.cookpad.prism.record;

import lombok.Getter;

public class UnsizedValueType implements ValueType {
    @Getter
    final private ValueKind valueKind;

    @Override
    public String toRedshiftTypeName() {
        return valueKind.getRedshiftTypeName();
    }

    public UnsizedValueType(ValueKind valueKind) {
        if (valueKind.isSized()) {
            throw new IllegalArgumentException("Given value kind is sized");
        }
        this.valueKind = valueKind;
    }
}
