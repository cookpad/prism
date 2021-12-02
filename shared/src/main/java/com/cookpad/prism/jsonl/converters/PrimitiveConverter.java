package com.cookpad.prism.jsonl.converters;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.parquet.io.api.Binary;
import com.cookpad.prism.record.values.PrimitiveValue;
import static com.cookpad.prism.record.values.PrimitiveValue.*;

public interface PrimitiveConverter<T extends PrimitiveValue<?>> {
    public T convertFrom(JsonNode node) throws UnexpectedValueType;

    public static class StringConverter implements PrimitiveConverter<BinaryValue> {
        final static ObjectMapper MAPPER = new ObjectMapper();
        @Override
        public BinaryValue convertFrom(JsonNode node) throws UnexpectedValueType {
            String str;
            if (node.isTextual()) {
                str = node.asText();
            } else {
                try {
                    str = MAPPER.writeValueAsString(node);
                } catch (JsonProcessingException e) {
                    throw new UnexpectedValueType("JSON");
                }
            }
            return new BinaryValue(Binary.fromString(str));
        }
    }

    public static class BooleanConverter implements PrimitiveConverter<BooleanValue> {
        @Override
        public BooleanValue convertFrom(JsonNode node) throws UnexpectedValueType {
            if (!node.isBoolean()) {
                throw new UnexpectedValueType("booelan");
            }
            return new BooleanValue(node.asBoolean());
        }
    }

    public static class IntegerConverter implements PrimitiveConverter<IntegerValue> {
        @Override
        public IntegerValue convertFrom(JsonNode node) throws UnexpectedValueType {
            if (!node.isIntegralNumber()) {
                throw new UnexpectedValueType("int");
            }
            return new IntegerValue(node.asInt());
        }
    }

    public static class BigintConverter implements PrimitiveConverter<LongValue> {
        @Override
        public LongValue convertFrom(JsonNode node) throws UnexpectedValueType {
            if (!node.isIntegralNumber()) {
                throw new UnexpectedValueType("long");
            }
            return new LongValue(node.asLong());
        }
    }

    public static class TimestampConverter implements PrimitiveConverter<LongValue> {
        public OffsetDateTime toOffsetDateTime(JsonNode value) throws UnexpectedValueType {
            if (!value.isTextual()) {
                throw new UnexpectedValueType("ISO8601 string");
            }
            String iso8601Text = value.asText();
            OffsetDateTime odt;
            try {
                odt = OffsetDateTime.parse(iso8601Text);
            } catch(DateTimeParseException ex) {
                throw new UnexpectedValueType("ISO8601 string");
            }
            if (odt.toInstant().compareTo(Instant.EPOCH) < 0) {
                ZoneOffset offset = odt.getOffset();
                odt = OffsetDateTime.ofInstant(Instant.EPOCH, offset);
            }
            return odt;
        }

        @Override
        public LongValue convertFrom(JsonNode value) throws UnexpectedValueType {
            OffsetDateTime odt = this.toOffsetDateTime(value);
            return new LongValue(odt.toInstant().toEpochMilli());
        }
    }

    public static class DoubleConverter implements PrimitiveConverter<DoubleValue> {
        @Override
        public DoubleValue convertFrom(JsonNode node) throws UnexpectedValueType {
            if (!node.isNumber()) {
                throw new UnexpectedValueType("double");
            }
            return new DoubleValue(node.asDouble());
        }
    }

    public static class FloatConverter implements PrimitiveConverter<FloatValue> {
        @Override
        public FloatValue convertFrom(JsonNode node) throws UnexpectedValueType {
            if (!node.isNumber()) {
                throw new UnexpectedValueType(node.asText() + "is not a float value.");
            }
            // node.isNumber() is always true here
            return new FloatValue(node.floatValue());
        }
    }
}
