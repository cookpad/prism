package com.cookpad.prism.jsonl.converters;

import com.fasterxml.jackson.databind.JsonNode;

import com.cookpad.prism.record.Schema.Column;
import com.cookpad.prism.record.values.Value;

public interface Converter {
    public Value convertFrom(Column column, JsonNode node) throws UnexpectedValueType;
}
