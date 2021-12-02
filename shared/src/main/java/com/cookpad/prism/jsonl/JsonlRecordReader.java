package com.cookpad.prism.jsonl;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;

import com.cookpad.prism.jsonl.converters.Converter;
import com.cookpad.prism.jsonl.converters.DefaultConverter;
import com.cookpad.prism.jsonl.converters.NonNullConverter;
import com.cookpad.prism.jsonl.converters.NullableConverter;
import com.cookpad.prism.jsonl.converters.PrimitiveConverter;
import com.cookpad.prism.record.Schema;
import com.cookpad.prism.record.ValueKind;
import com.cookpad.prism.record.ValueListRecord;
import com.cookpad.prism.record.Schema.Column;
import com.cookpad.prism.record.partitioned.DateAttachedRecord;
import com.cookpad.prism.record.values.Value;
import com.cookpad.prism.record.values.PrimitiveValue.LongValue;

import static com.cookpad.prism.jsonl.converters.PrimitiveConverter.*;

public class JsonlRecordReader implements AutoCloseable {
    private static final StringConverter STRING_CONVERTER = new StringConverter();
    private static final BooleanConverter BOOLEAN_CONVERTER = new BooleanConverter();
    private static final IntegerConverter INTEGER_CONVERTER = new IntegerConverter();
    private static final BigintConverter BIGINT_CONVERTER = new BigintConverter();
    private static final TimestampConverter TIMESTAMP_CONVERTER = new TimestampConverter();
    private static final DoubleConverter DOUBLE_CONVERTER = new DoubleConverter();
    private static final FloatConverter FLOAT_CONVERTER = new FloatConverter();
    private static final Map<ValueKind, PrimitiveConverter<?>> CONVERTERS_BY_TYPE;
    static {
        CONVERTERS_BY_TYPE = new HashMap<>();
        CONVERTERS_BY_TYPE.put(ValueKind.STRING, STRING_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.BOOLEAN, BOOLEAN_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.INTEGER, INTEGER_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.BIGINT, BIGINT_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.TIMESTAMP, TIMESTAMP_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.DOUBLE, DOUBLE_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.REAL, FLOAT_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.DATE, STRING_CONVERTER);
        CONVERTERS_BY_TYPE.put(ValueKind.SMALLINT, INTEGER_CONVERTER);
    };

    final private Schema schema;
    final private JsonlReader inner;
    final private List<Map.Entry<Column, Converter>> columns;

    final private OffsetDateTime DEFAULT_TIMESTAMP = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

    public JsonlRecordReader(Schema schema, JsonlReader inner) {
        this.schema = schema;
        this.inner = inner;
        Column tsCol = schema.getTimestampColumn();
        this.columns = schema.getColumns().stream().map((col) -> {
            PrimitiveConverter<?> primitive = CONVERTERS_BY_TYPE.get(col.getValueType().getValueKind());
            if (col.isNullable()) {
                return new AbstractMap.SimpleEntry<Column, Converter>(col, new NullableConverter(primitive));
            } else {
                if (col == tsCol) {
                    LongValue defaultValue = new LongValue(DEFAULT_TIMESTAMP.toInstant().toEpochMilli());
                    return new AbstractMap.SimpleEntry<Column, Converter>(col, new DefaultConverter(primitive, defaultValue));
                } else {
                    return new AbstractMap.SimpleEntry<Column, Converter>(col, new NonNullConverter(primitive));
                }
            }
        }).collect(Collectors.toList());
    }

    private Optional<OffsetDateTime> tryToReadTimetamp(JsonNode tsNode) {
        if (tsNode == null || tsNode.isNull()) {
            return Optional.empty();
        }
        return Optional.of(TIMESTAMP_CONVERTER.toOffsetDateTime(tsNode));
    }

    public DateAttachedRecord read() throws IOException {
        JsonNode objJsonNode = this.inner.read();
        if (objJsonNode == null) {
            return null;
        }

        List<JsonNode> nodes = this.columns.stream().map((pair) -> {
            Column col = pair.getKey();
            JsonNode node = objJsonNode.get(col.getName());
            return node;
        }).collect(Collectors.toList());

        JsonNode tsNode = nodes.get(Schema.TIMESTAMP_INDEX);
        if (tsNode == null || tsNode.isNull()) {
            // Fill it with seconday timestamp column's value
            for (Column column : this.schema.getSecondaryTimestampColumns()) {
                JsonNode stsNode = nodes.get(column.getIndex());
                if (stsNode != null && !stsNode.isNull()) {
                    tsNode = stsNode;
                    break;
                }
            }
        }
        OffsetDateTime timestamp = tryToReadTimetamp(tsNode).orElse(DEFAULT_TIMESTAMP);
        nodes.set(Schema.TIMESTAMP_INDEX, tsNode);

        List<Value> values = this.columns.stream().map((pair) -> {
            Column col = pair.getKey();
            Converter conv = pair.getValue();
            JsonNode node = nodes.get(col.getIndex());
            return conv.convertFrom(col, node);
        }).collect(Collectors.toList());

        LocalDate dt = timestamp.toLocalDate();
        return new DateAttachedRecord(new ValueListRecord(values), dt);
    }

    @Override
    public void close() throws IOException {
        this.inner.close();
    }
}
