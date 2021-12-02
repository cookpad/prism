package com.cookpad.prism.jsonl;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.ZoneOffset;

import com.cookpad.prism.record.Schema;
import com.cookpad.prism.record.UnsizedValueType;
import com.cookpad.prism.record.ValueKind;
import com.cookpad.prism.record.Schema.Builder.BadSchemaError;
import com.cookpad.prism.record.values.NonNullValue;
import com.cookpad.prism.record.values.NullValue;
import com.cookpad.prism.record.values.PrimitiveValue;
import org.junit.jupiter.api.Test;

public class JsonlRecordReaderTest {
    @Test
    void readTimestampCompletedData() throws BadSchemaError, IOException {
        var schema = new Schema.Builder("dummy_schema", "dummy_table")
            .addTimestampColumn("utc_event_time", new UnsizedValueType(ValueKind.TIMESTAMP), true, ZoneOffset.ofHours(9))
            .build();
        var source =
            "{\"utc_event_time\":\"2018-10-24T09:00:23+09:00\"}\n" +
            "{\"utc_event_time\":\"2018-10-25T13:16:00+09:00\"}\n";
        try (
            var sr = new StringReader(source);
            var lnr = new LineNumberReader(sr);
            var reader = new JsonlReader(lnr);
            var recordReader = new JsonlRecordReader(schema, reader)
        ) {
            var record1 = recordReader.read();
            assertEquals(LocalDate.of(2018, 10, 24), record1.getPartitionDate());
            assertEquals(
                new NonNullValue(schema.getColumns().get(0), new PrimitiveValue.LongValue(1540339223000L)),
                record1.getValue(0));
            var record2 = recordReader.read();
            assertEquals(LocalDate.of(2018, 10, 25), record2.getPartitionDate());
            assertEquals(
                new NonNullValue(schema.getColumns().get(0), new PrimitiveValue.LongValue(1540440960000L)),
                record2.getValue(0));
            var eof = recordReader.read();
            assertNull(eof);
        }
    }

    @Test
    void readNoTimestampDataWithNoSecondaryTimestampColumn() throws BadSchemaError, IOException {
        var schema = new Schema.Builder("dummy_schema", "dummy_table")
            .addTimestampColumn("utc_event_time", new UnsizedValueType(ValueKind.TIMESTAMP), true, ZoneOffset.ofHours(9))
            .addColumn("jst_time", new UnsizedValueType(ValueKind.TIMESTAMP), true)
            .build();
        var source =
            "{\"jst_time\":\"2018-10-24T09:00:23+09:00\"}\n" +
            "{\"jst_time\":\"2018-10-25T13:16:00+09:00\"}\n";
        try (
            var sr = new StringReader(source);
            var lnr = new LineNumberReader(sr);
            var reader = new JsonlReader(lnr);
            var recordReader = new JsonlRecordReader(schema, reader)
        ) {
            var record1 = recordReader.read();
            assertEquals(LocalDate.of(1970, 1, 1), record1.getPartitionDate());
            assertEquals(
                new NullValue(),
                record1.getValue(0));
            var record2 = recordReader.read();
            assertEquals(LocalDate.of(1970, 1, 1), record2.getPartitionDate());
            assertEquals(
                new NullValue(),
                record2.getValue(0));
            var eof = recordReader.read();
            assertNull(eof);
        }
    }

    @Test
    void readNoTimestampDataWithSecondaryTimestampColumn() throws BadSchemaError, IOException {
        var schema = new Schema.Builder("dummy_schema", "dummy_table")
            .addTimestampColumn("utc_event_time", new UnsizedValueType(ValueKind.TIMESTAMP), true, ZoneOffset.ofHours(9))
            .addSecondaryTimestampColumn("jst_time", new UnsizedValueType(ValueKind.TIMESTAMP), true)
            .build();
        var source =
            "{\"jst_time\":\"2018-10-24T09:00:23+09:00\"}\n" +
            "{\"jst_time\":\"2018-10-25T13:16:00+09:00\"}\n";
        try (
            var sr = new StringReader(source);
            var lnr = new LineNumberReader(sr);
            var reader = new JsonlReader(lnr);
            var recordReader = new JsonlRecordReader(schema, reader)
        ) {
            var record1 = recordReader.read();
            assertEquals(LocalDate.of(2018, 10, 24), record1.getPartitionDate());
            assertEquals(
                new NonNullValue(schema.getColumns().get(0), new PrimitiveValue.LongValue(1540339223000L)),
                record1.getValue(0));
            var record2 = recordReader.read();
            assertEquals(LocalDate.of(2018, 10, 25), record2.getPartitionDate());
            assertEquals(
                new NonNullValue(schema.getColumns().get(0), new PrimitiveValue.LongValue(1540440960000L)),
                record2.getValue(0));
            var eof = recordReader.read();
            assertNull(eof);
        }
    }
}
