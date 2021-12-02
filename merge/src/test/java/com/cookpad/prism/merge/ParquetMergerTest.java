package com.cookpad.prism.merge;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import com.cookpad.prism.record.Record;
import com.cookpad.prism.record.Schema;
import com.cookpad.prism.record.UnsizedValueType;
import com.cookpad.prism.record.ValueKind;
import com.cookpad.prism.record.ValueListRecord;
import com.cookpad.prism.record.Schema.Builder.BadSchemaError;
import com.cookpad.prism.record.values.NonNullValue;
import com.cookpad.prism.record.values.PrimitiveValue;
import com.cookpad.prism.record.values.Value;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

import lombok.val;

public class ParquetMergerTest {
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

        @SuppressWarnings("unchecked")
        ParquetReader<Record> readerA = mock(ParquetReader.class);
        @SuppressWarnings("unchecked")
        ParquetReader<Record> readerB = mock(ParquetReader.class);
        when(readerA.read())
            .thenReturn(record1)
            .thenReturn(record3)
            .thenReturn(null)
        ;
        when(readerB.read())
            .thenReturn(record2)
            .thenReturn(null)
        ;

        @SuppressWarnings("unchecked")
        ParquetWriter<Record> writer = mock(ParquetWriter.class);

        new ParquetMerger(readerA, readerB, writer).merge();

        val inorder = inOrder(writer);
        inorder.verify(writer).write(record1);
        inorder.verify(writer).write(record2);
        inorder.verify(writer).write(record3);
    }

    @Test
    public void testMergeInterleaveRev() throws BadSchemaError, IOException {
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

        @SuppressWarnings("unchecked")
        ParquetReader<Record> readerA = mock(ParquetReader.class);
        @SuppressWarnings("unchecked")
        ParquetReader<Record> readerB = mock(ParquetReader.class);
        when(readerA.read())
            .thenReturn(record2)
            .thenReturn(null)
        ;
        when(readerB.read())
            .thenReturn(record1)
            .thenReturn(record3)
            .thenReturn(null)
        ;

        @SuppressWarnings("unchecked")
        ParquetWriter<Record> writer = mock(ParquetWriter.class);

        new ParquetMerger(readerA, readerB, writer).merge();

        val inorder = inOrder(writer);
        inorder.verify(writer).write(record1);
        inorder.verify(writer).write(record2);
        inorder.verify(writer).write(record3);
    }

    @Test
    public void testMergeSameValue() throws BadSchemaError, IOException {
        val schema = new Schema.Builder("test_s", "test_t")
            .withTimestamp("time", false, ZoneOffset.UTC)
            .addColumn("value", new UnsizedValueType(ValueKind.BIGINT), false)
            .build()
        ;
        val tsColumn = schema.getColumns().get(0);
        val valueColumn = schema.getColumns().get(1);

        val values1a = new ArrayList<Value>();
        values1a.add(new NonNullValue(tsColumn, new PrimitiveValue.LongValue(1L)));
        values1a.add(new NonNullValue(valueColumn, new PrimitiveValue.LongValue(200L)));
        val record1a = new ValueListRecord(values1a);

        val values1b = new ArrayList<Value>();
        values1b.add(new NonNullValue(tsColumn, new PrimitiveValue.LongValue(1L)));
        values1b.add(new NonNullValue(valueColumn, new PrimitiveValue.LongValue(100L)));
        val record1b = new ValueListRecord(values1b);

        @SuppressWarnings("unchecked")
        ParquetReader<Record> readerA = mock(ParquetReader.class);
        @SuppressWarnings("unchecked")
        ParquetReader<Record> readerB = mock(ParquetReader.class);
        when(readerA.read())
            .thenReturn(record1a)
            .thenReturn(null)
        ;
        when(readerB.read())
            .thenReturn(record1b)
            .thenReturn(null)
        ;

        @SuppressWarnings("unchecked")
        ParquetWriter<Record> writer = mock(ParquetWriter.class);

        new ParquetMerger(readerA, readerB, writer).merge();

        val inorder = inOrder(writer);
        inorder.verify(writer).write(record1a);
        inorder.verify(writer).write(record1b);
    }
}
