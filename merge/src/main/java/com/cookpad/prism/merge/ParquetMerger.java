package com.cookpad.prism.merge;

import java.io.IOException;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import com.cookpad.prism.record.Record;
import com.cookpad.prism.record.RecordTimestampComparator;

import lombok.RequiredArgsConstructor;

// input: parquet temp file object list
// output: merged parquet temp file object list
@RequiredArgsConstructor
public class ParquetMerger {
    private final ParquetReader<Record> readerA;
    private final ParquetReader<Record> readerB;
    private final ParquetWriter<Record> writer;

    public void merge() throws IOException {
        RecordTimestampComparator comparator = new RecordTimestampComparator();
        Record recordA = this.readerA.read();
        Record recordB = this.readerB.read();
        while (true) {
            int cmp = comparator.compare(recordA, recordB);
            if (cmp > 0) {
                this.writer.write(recordB);
                if (recordA == null) {
                    this.writeSingle(this.readerB, this.writer);
                    break;
                }
                recordB = this.readerB.read();
            } else if (cmp < 0) {
                this.writer.write(recordA);
                if (recordB == null) {
                    this.writeSingle(this.readerA, this.writer);
                    break;
                }
                recordA = this.readerA.read();
            } else {
                if (recordA == null) {
                    break;
                }
                this.writer.write(recordA);
                this.writer.write(recordB);
                recordA = this.readerA.read();
                recordB = this.readerB.read();
            }
        }
    }

    private void writeSingle(ParquetReader<Record> reader, ParquetWriter<Record> writer) throws IOException {
        Record record;
        while ((record = reader.read()) != null) {
            writer.write(record);
        }
    }
}
