package com.cookpad.prism.merge;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import com.cookpad.prism.TempFile;
import com.cookpad.prism.record.Record;
import com.cookpad.prism.record.RecordReaderFactory;
import com.cookpad.prism.record.RecordWriterFactory;
import com.cookpad.prism.record.Schema;
import com.cookpad.prism.record.UnsizedValueType;
import com.cookpad.prism.record.ValueKind;
import com.cookpad.prism.record.ValueListRecord;
import com.cookpad.prism.record.Schema.Builder.BadSchemaError;
import com.cookpad.prism.record.values.NonNullValue;
import com.cookpad.prism.record.values.PrimitiveValue;
import com.cookpad.prism.record.values.Value;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import lombok.val;

public class ParallelParquetMergerTest {
    @Test
    public void testMergeInterleave() throws BadSchemaError, IOException {
        val schema = new Schema.Builder("test_s", "test_t")
            .withTimestamp("time", false, ZoneOffset.UTC)
            .addColumn("value", new UnsizedValueType(ValueKind.BIGINT), false)
            .build()
        ;
        val tsColumn = schema.getColumns().get(0);
        val valueColumn = schema.getColumns().get(1);

        val values1 = new ArrayList<Value>();
        values1.add(new NonNullValue(tsColumn, new PrimitiveValue.LongValue(1L)));
        values1.add(new NonNullValue(valueColumn, new PrimitiveValue.LongValue(100L)));
        val record1 = new ValueListRecord(values1);

        val values2 = new ArrayList<Value>();
        values2.add(new NonNullValue(tsColumn, new PrimitiveValue.LongValue(2L)));
        values2.add(new NonNullValue(valueColumn, new PrimitiveValue.LongValue(200L)));
        val record2 = new ValueListRecord(values2);

        val values3 = new ArrayList<Value>();
        values3.add(new NonNullValue(tsColumn, new PrimitiveValue.LongValue(3L)));
        values3.add(new NonNullValue(valueColumn, new PrimitiveValue.LongValue(300L)));
        val record3 = new ValueListRecord(values3);

        val conf = new Configuration();

        val tmp1 = new TempFile("prism-merge-test-", ".parquet");
        val tmp2 = new TempFile("prism-merge-test-", ".parquet");
        val tmp3 = new TempFile("prism-merge-test-", ".parquet");

        val writerFactory = new RecordWriterFactory(conf);
        val readerFactory = new RecordReaderFactory(conf);
        ParquetWriter<Record> writerA = writerFactory.build(schema, tmp1.getPath());
        writerA.write(record1);
        writerA.close();

        ParquetWriter<Record> writerB = writerFactory.build(schema, tmp2.getPath());
        writerB.write(record2);
        writerB.close();

        ParquetWriter<Record> writerC = writerFactory.build(schema, tmp3.getPath());
        writerC.write(record3);
        writerC.close();

        val ex = Executors.newFixedThreadPool(4);
        val parquetFileMerger = new ParquetFileMerger(writerFactory, readerFactory);
        val parallelMerger = new ParallelParquetMerger(ex, ex, parquetFileMerger);

        val suppliers = new ArrayList<Supplier<TempFile>>();
        suppliers.add(() -> tmp2);
        suppliers.add(() -> tmp1);
        suppliers.add(() -> tmp3);
        val out = parallelMerger.merge(schema, suppliers);

        val reader = readerFactory.build(schema, out.getPath());
        assertEquals(record1, reader.read());
        assertEquals(record2, reader.read());
        assertEquals(record3, reader.read());
        assertEquals(null, reader.read());

        tmp1.close();
        tmp2.close();
        tmp3.close();
    }
}
