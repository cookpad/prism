package com.cookpad.prism.record;

import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum ValueKind {
    STRING(PrimitiveTypeName.BINARY, "varchar", true) {
        @Override
        public OriginalType getOriginalType() {
            return OriginalType.UTF8;
        }
    },
    BOOLEAN(PrimitiveTypeName.BOOLEAN, "boolean", false),
    INTEGER(PrimitiveTypeName.INT32, "int", false),
    BIGINT(PrimitiveTypeName.INT64, "bigint", false),
    TIMESTAMP(PrimitiveTypeName.INT64, "timestamp", false) {
        @Override
        public OriginalType getOriginalType() {
            return OriginalType.TIMESTAMP_MILLIS;
        }
    },
    SMALLINT(PrimitiveTypeName.INT32, "smallint", false),
    DATE(PrimitiveTypeName.BINARY, "varchar(10)", false),
    DOUBLE(PrimitiveTypeName.DOUBLE, "double", false),
    REAL(PrimitiveTypeName.FLOAT, "float", false),
    ;

    @Getter
    final private PrimitiveTypeName primitiveType;
    @Getter
    final private String redshiftTypeName;
    @Getter
    final private boolean isSized;

    public OriginalType getOriginalType() {
        return null;
    }
}
