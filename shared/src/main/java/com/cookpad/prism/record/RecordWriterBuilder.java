package com.cookpad.prism.record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

public class RecordWriterBuilder extends ParquetWriter.Builder<Record, RecordWriterBuilder> {
    private Schema schema;

    public RecordWriterBuilder withSchema(Schema schema) {
        this.schema = schema;
        return this;
    }

    public RecordWriterBuilder(Path file) {
        super(file);
    }

    @Override
    protected RecordWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<Record> getWriteSupport(Configuration conf) {
        return new RecordWriteSupport(this.schema);
    }
}
