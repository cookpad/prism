package com.cookpad.prism.record;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Type.Repetition;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
public class Schema {
    public static final int TIMESTAMP_INDEX = 0;

    @Getter
    @NonNull
    final private String schemaName;
    @Getter
    @NonNull
    final private String tableName;
    @Getter
    @NonNull
    final private List<Column> columns;
    @Getter
    @NonNull
    final private ZoneOffset zoneOffset;
    @Getter
    @NonNull
    private final List<Column> secondaryTimestampColumns;

    public Column getTimestampColumn() {
        return this.columns.get(TIMESTAMP_INDEX);
    }

    public MessageType toMessageType() {
        List<Type> fields = this.getColumns()
            .stream()
            .map(Column::toFieldType)
            .collect(Collectors.toList())
        ;
        String fullName = String.format("%s.%s", this.getSchemaName(), this.getTableName());
        return new MessageType(fullName, fields);
    }

    public static class Builder {
        private String logicalSchemaName;
        private String logicalTableName;
        private List<Column> columns;
        private ZoneOffset zoneOffset;
        private List<Column> secondaryTimestampColumns;

        public Builder(String logicalSchemaName, String logicalTableName) {
            this.logicalSchemaName = logicalSchemaName;
            this.logicalTableName = logicalTableName;
            columns = new ArrayList<>();
            columns.add(null); // Depends on TIMESTAMP_INDEX
            this.secondaryTimestampColumns = new ArrayList<>();
        }

        public Builder addColumn(String name, ValueType type, boolean isNullable) {
            Column column = new Column(this.columns.size(), name, type, isNullable, false);
            this.columns.add(column);
            return this;
        }

        public Builder addSecondaryTimestampColumn(String name, ValueType type, boolean isNullable) {
            Column column = new Column(this.columns.size(), name, type, isNullable, true);
            this.columns.add(column);
            this.secondaryTimestampColumns.add(column);
            return this;
        }

        public Builder addTimestampColumn(String name, ValueType type, boolean isNullable, ZoneOffset zoneOffset) throws BadSchemaError {
            if (!type.getValueKind().equals(ValueKind.TIMESTAMP)) {
                throw new BadSchemaError("Partition columns' type must be timestamp");
            }
            this.withTimestamp(name, isNullable, zoneOffset);
            return this;
        }

        public Builder withTimestamp(String name, boolean isNullable, ZoneOffset zoneOffset) throws BadSchemaError {
            if (this.columns.get(TIMESTAMP_INDEX) != null) {
                throw new BadSchemaError("Timestamp column was already defined");
            }
            this.zoneOffset = zoneOffset;
            // true timestamp column is not a secondary timestamp
            this.columns.set(TIMESTAMP_INDEX, new Column(TIMESTAMP_INDEX, name, new UnsizedValueType(ValueKind.TIMESTAMP), isNullable, false));
            return this;
        }

        public Schema build() throws BadSchemaError {
            if (this.columns.get(TIMESTAMP_INDEX) == null) {
                throw new BadSchemaError("Timestamp column is not configured");
            }
            return new Schema(
                this.logicalSchemaName,
                this.logicalTableName,
                this.columns,
                this.zoneOffset,
                this.secondaryTimestampColumns
            );
        }

        @SuppressWarnings("serial")
        public static class BadSchemaError extends Exception {
            BadSchemaError(String message) {
                super(message);
            }
        }
    }

    @ToString
    public static class Column {
        @Getter
        final private int index;
        @Getter
        final private String name;
        @Getter
        final private ValueType valueType;
        @Getter
        final private boolean isNullable;
        @Getter
        final private boolean isSecondaryTimestamp;

        public Column(int index, String name, ValueType valueType, boolean isNullable, boolean isSecondaryTimestamp) {
            this.index = index;
            this.name = name;
            this.valueType = valueType;
            this.isNullable = isNullable;
            this.isSecondaryTimestamp = isSecondaryTimestamp;
        }

        public Type toFieldType() {
            ValueKind valueType = this.getValueType().getValueKind();
            return Types.primitive(valueType.getPrimitiveType(), this.getRepetition())
                .as(valueType.getOriginalType())
                .named(this.getName());
        }

        private Repetition getRepetition() {
            if (this.isNullable()) {
                return Repetition.OPTIONAL;
            } else {
                return Repetition.REQUIRED;
            }
        }

        public void startField(RecordConsumer consumer) {
            consumer.startField(this.getName(), this.getIndex());
        }

        public void endField(RecordConsumer consumer) {
            consumer.endField(this.getName(), this.getIndex());
        }
    }
}
