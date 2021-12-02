package com.cookpad.prism.record;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RecordReaderFactory {
    private final Configuration conf;

    public ParquetReader<Record> build(Schema schema, Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString());
        RecordReadSupport readSupport = new RecordReadSupport(schema);
        ParquetReader.Builder<Record> builder = ParquetReader.builder(readSupport, hadoopPath).withConf(this.conf);
        return builder.build();
    }
}
