package com.cookpad.prism.record;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import com.cookpad.prism.record.Schema.Column;
import com.cookpad.prism.record.values.NonNullValue;
import com.cookpad.prism.record.values.NullValue;
import com.cookpad.prism.record.values.Value;

import static com.cookpad.prism.record.values.PrimitiveValue.*;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class RecordConverter extends GroupConverter {
    private final Schema schema;
    @Getter
    private List<Value> currentValues;
    private final List<SimplePrimitiveConverter> converters;

    public RecordConverter(@NonNull Schema schema) {
        this.schema = schema;
        this.initCurrentValues();
        this.converters = this.schema.getColumns()
            .stream()
            .map((col) -> new SimplePrimitiveConverter(this, col))
            .collect(Collectors.toList());
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return this.converters.get(fieldIndex);
    }

    @Override
    public void start() {
        this.initCurrentValues();
    }

    @Override
    public void end() {
        // Do nothing
    }

    private void initCurrentValues() {
        this.currentValues = this.schema.getColumns()
            .stream()
            .map((column) -> new NullValue())
            .collect(Collectors.toList())
        ;
    }

    public Record getCurrentRecord() {
        return new ValueListRecord(this.currentValues);
    }

    @RequiredArgsConstructor
    private static class SimplePrimitiveConverter extends PrimitiveConverter {
        final private RecordConverter parent;
        final private Column column;

        private void setValue(Value value) {
            this.parent.getCurrentValues().set(this.column.getIndex(), value);
        }

        @Override
        public void addBinary(Binary value) {
            this.setValue(new NonNullValue(this.column, new BinaryValue(value)));
        }

        @Override
        public void addBoolean(boolean value) {
            this.setValue(new NonNullValue(this.column, new BooleanValue(value)));
        }

        @Override
        public void addInt(int value) {
            this.setValue(new NonNullValue(this.column, new IntegerValue(value)));
        }

        @Override
        public void addLong(long value) {
            this.setValue(new NonNullValue(this.column, new LongValue(value)));
        }

        @Override
        public void addDouble(double value) {
            this.setValue(new NonNullValue(this.column, new DoubleValue(value)));
        }

        @Override
        public void addFloat(float value) {
            this.setValue(new NonNullValue(this.column, new FloatValue(value)));
        }
    }
}
