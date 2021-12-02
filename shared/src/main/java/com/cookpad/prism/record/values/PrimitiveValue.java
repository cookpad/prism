package com.cookpad.prism.record.values;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

public interface PrimitiveValue<T> {
    public T getValue();
    public void writeValue(RecordConsumer consumer);

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class BinaryValue implements PrimitiveValue<Binary> {
        @Getter
        @NonNull
        final private Binary value;

        @Override
        public void writeValue(RecordConsumer consumer) {
            consumer.addBinary(this.getValue());
        }
    }

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class BooleanValue implements PrimitiveValue<Boolean> {
        @Getter
        @NonNull
        final private Boolean value;

        @Override
        public void writeValue(RecordConsumer consumer) {
            consumer.addBoolean(this.getValue());
        }
    }

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class IntegerValue implements PrimitiveValue<Integer> {
        @Getter
        @NonNull
        final private Integer value;

        @Override
        public void writeValue(RecordConsumer consumer) {
            consumer.addInteger(this.getValue());
        }
    }

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class LongValue implements PrimitiveValue<Long> {
        @Getter
        @NonNull
        final private Long value;

        @Override
        public void writeValue(RecordConsumer consumer) {
            consumer.addLong(this.getValue());
        }
    }

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class DoubleValue implements PrimitiveValue<Double> {
        @Getter
        @NonNull
        final private Double value;

        @Override
        public void writeValue(RecordConsumer consumer) {
            consumer.addDouble(this.getValue());
        }
    }

    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class FloatValue implements PrimitiveValue<Float> {
        @Getter
        @NonNull
        final private Float value;

        @Override
        public void writeValue(RecordConsumer consumer) {
            consumer.addFloat(this.getValue());
        }
    }
}
