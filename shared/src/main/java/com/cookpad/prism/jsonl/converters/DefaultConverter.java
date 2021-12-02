package com.cookpad.prism.jsonl.converters;

import com.fasterxml.jackson.databind.JsonNode;

import com.cookpad.prism.record.Schema.Column;
import com.cookpad.prism.record.values.NonNullValue;
import com.cookpad.prism.record.values.PrimitiveValue;
import com.cookpad.prism.record.values.Value;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultConverter implements Converter {
    final private PrimitiveConverter<?> primitiveConverter;
    @NonNull
    final private PrimitiveValue<?> defaultValue;

    @Override
    public Value convertFrom(Column column, JsonNode node) throws UnexpectedValueType {
        if (node == null || node.isNull()) {
            return new NonNullValue(column, this.defaultValue);
        }
        PrimitiveValue<?> value = this.primitiveConverter.convertFrom(node);
        return new NonNullValue(column, value);
    }
}
