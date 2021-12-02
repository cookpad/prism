package com.cookpad.prism.record.partitioned;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;

import org.apache.parquet.hadoop.ParquetWriter;
import com.cookpad.prism.TempFile;
import com.cookpad.prism.record.Record;
import com.cookpad.prism.record.RecordWriterFactory;
import com.cookpad.prism.record.Schema;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PartitionedWriter implements PartitionedRecordWriter {
    private final RecordWriterFactory recordWriterFactory;
    private final TempFile.Factory tempFileFactory;
    private final PartitionCollector partitionCollector;
    private final Schema schema;

    @Getter
    final private TreeMap<LocalDate, Partition> partitions = new TreeMap<>();

    private ParquetWriter<Record> route(PartitionedRecord record) throws IOException {
        LocalDate date = record.getPartitionDate();
        Partition dest = this.partitions.get(date);
        if (dest == null) {
            TempFile tempFile = this.tempFileFactory.create();
            ParquetWriter<Record> writer = recordWriterFactory.build(this.schema, tempFile.getPath());
            dest = new Partition(tempFile, writer);
            this.partitions.put(date, dest);
        }
        return dest.getWriter();
    }

    public void write(PartitionedRecord record) throws IOException {
        ParquetWriter<Record> dest = this.route(record);
        dest.write(record);
    }

    @Override
    public void close() throws IOException {
        TreeMap<LocalDate, TempFile> partitionToTempFile = new TreeMap<>();
        for (Map.Entry<LocalDate, Partition> kv: this.partitions.entrySet()) {
            kv.getValue().getWriter().close();
            partitionToTempFile.put(kv.getKey(), kv.getValue().getTempFile());
        }
        this.partitionCollector.commit(partitionToTempFile);
    }

    @Data
    private static class Partition {
        private final TempFile tempFile;
        private final ParquetWriter<Record> writer;
    }
}
