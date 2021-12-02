package com.cookpad.prism.record;

public interface ValueType {
    public String toRedshiftTypeName();
    public ValueKind getValueKind();
}
