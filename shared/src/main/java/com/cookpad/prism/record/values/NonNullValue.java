package com.cookpad.prism.record.values;

import org.apache.parquet.io.api.RecordConsumer;
import com.cookpad.prism.record.Schema.Column;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
@EqualsAndHashCode
public class NonNullValue implements Value {
    final private Column column;
    @Getter
    final private PrimitiveValue<?> inner;

    @Override
    public void writeField(RecordConsumer consumer) {
        try {
            this.column.startField(consumer);
            this.inner.writeValue(consumer);
            this.column.endField(consumer);
        } catch (Exception e) {
            String columnName = column.getName();
            Object value = inner.getValue();
            throw new WriteFieldException(columnName, value, e);
        }
    }
}
