package com.cookpad.prism.record.partitioned;

import java.io.IOException;
import java.time.LocalDate;
import java.util.TreeMap;

import org.apache.parquet.hadoop.ParquetWriter;
import com.cookpad.prism.TempFile;
import com.cookpad.prism.record.Record;
import com.cookpad.prism.record.RecordWriterFactory;
import com.cookpad.prism.record.Schema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SortedPartitionedWriter implements PartitionedRecordWriter {
    private final RecordWriterFactory recordWriterFactory;
    private final TempFile.Factory tempFileFactory;
    private final PartitionCollector partitionCollector;
    private final Schema schema;

    private LocalDate currentDate = null;
    private ParquetWriter<Record> currentWriter = null;

    @Getter
    private final TreeMap<LocalDate, TempFile> partitions = new TreeMap<>();

    @Override
    public void write(PartitionedRecord record) throws IOException {
        if (this.currentDate == null || record.getPartitionDate().compareTo(this.currentDate) > 0) {
            this.switchWriter(record.getPartitionDate());
        } else if (record.getPartitionDate().compareTo(this.currentDate) < 0) {
            throw new IllegalStateException("Partition date is unordered");
        }
        this.currentWriter.write(record);
    }

    private void switchWriter(LocalDate newDate) throws IOException {
        if (this.currentWriter != null) {
            this.currentWriter.close();
        }
        TempFile tempFile = this.tempFileFactory.create();
        this.currentWriter = this.recordWriterFactory.build(this.schema, tempFile.getPath());
        this.currentDate = newDate;
        this.partitions.put(newDate, tempFile);
    }

    @Override
    public void close() throws IOException {
        if (this.currentWriter != null) {
            this.currentWriter.close();
        }
        this.partitionCollector.commit(this.partitions);
    }
}
