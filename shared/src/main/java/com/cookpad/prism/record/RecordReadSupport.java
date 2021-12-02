package com.cookpad.prism.record;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RecordReadSupport extends ReadSupport<Record> {
    @NonNull
    final private Schema schema;

    @Override
    public RecordMaterializer<Record> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
            MessageType fileSchema, ReadContext readContext) {
        return new PrismRecordMaterializer(this.schema);
    }

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(this.schema.toMessageType());
    }
}
