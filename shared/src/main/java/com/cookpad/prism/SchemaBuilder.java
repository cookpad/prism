package com.cookpad.prism;

import java.util.List;
import java.util.Objects;

import com.cookpad.prism.record.Schema;
import com.cookpad.prism.record.SizedValueType;
import com.cookpad.prism.record.UnsizedValueType;
import com.cookpad.prism.record.ValueKind;
import com.cookpad.prism.record.ValueType;
import com.cookpad.prism.record.Schema.Builder.BadSchemaError;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.StreamColumn;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SchemaBuilder {
    private ValueKind getValueKindFromTypeName(String typeName) {
        ValueKind valueKind = ValueKind.valueOf(typeName.toUpperCase());
        if (valueKind == null) {
            throw new IllegalArgumentException("No such ValueKind: " + typeName);
        }
        return valueKind;
    }

    public Schema build(PrismTable table, List<StreamColumn> columns) throws BadColumnsError {
        StreamColumn partitionSourceColumn = null;
        for (StreamColumn column: columns) {
            if (column.isPartitionSource()) {
                partitionSourceColumn = column;
                break;
            }
        }
        if (partitionSourceColumn == null) {
            throw new BadColumnsError("no partition source column");
        }
        try {
            Schema.Builder builder = new Schema.Builder(table.getLogicalSchemaName(), table.getLogicalTableName());
            for (StreamColumn column: columns) {
                if (column.getType().toUpperCase().equals("UNKNOWN")) {
                    continue;
                }
                ValueKind valueKind = getValueKindFromTypeName(column.getType());
                ValueType valueType;
                if (column.getLength() == null) {
                    valueType = new UnsizedValueType(valueKind);
                } else {
                    valueType = new SizedValueType(valueKind, column.getLength());
                }
                if (column.isPartitionSource()) {
                    builder.addTimestampColumn(column.getName(), valueType, false, column.getZoneOffsetAsZoneOffset());
                } else {
                    if (this.isCompatible(partitionSourceColumn, column)) {
                        builder.addSecondaryTimestampColumn(column.getName(), valueType, true);
                    } else {
                        builder.addColumn(column.getName(), valueType, true);
                    }
                }
            }
            return builder.build();
        } catch(BadSchemaError cause) {
            throw new BadColumnsError(cause);
        }
    }

    public boolean isCompatible(StreamColumn a, StreamColumn b) {
        return (
            Objects.equals(a.getSourceName(),   b.getSourceName()) &&
            Objects.equals(a.getType(),         b.getType()) &&
            Objects.equals(a.getLength(),       b.getLength()) &&
            Objects.equals(a.getZoneOffset(),   b.getZoneOffset()) &&
            Objects.equals(a.getSourceOffset(), b.getSourceOffset())
        );
    }

    @SuppressWarnings("serial")
    public static class BadColumnsError extends Exception {
        BadColumnsError(Throwable cause) {
            super(cause);
        }

        BadColumnsError(String message) {
            super(message);
        }
    }
}
