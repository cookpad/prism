package com.cookpad.prism.record;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RecordWriterFactory {
    private final Configuration conf;

    public ParquetWriter<Record> build(Schema schema, Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toAbsolutePath().toString());
        RecordWriterBuilder builder = new RecordWriterBuilder(hadoopPath)
            .withConf(this.conf)
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(Mode.OVERWRITE)
        ;
        return builder.build();
    }
}
