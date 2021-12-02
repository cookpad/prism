package com.cookpad.prism.record;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import com.cookpad.prism.record.values.Value.WriteFieldException;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RecordWriteSupport extends WriteSupport<Record> {
    final private Schema schema;
    private RecordConsumer consumer;

    @Override
    public WriteContext init(Configuration configuration) {
        MessageType messageType = schema.toMessageType();
        Map<String, String> metadata = new HashMap<>();
        return new WriteContext(messageType, metadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.consumer = recordConsumer;
    }

    @Override
    public void write(Record record) {
        try {
            record.writeMessage(this.consumer);
        } catch (WriteFieldException e) {
            throw new WriteRecordException(this.schema.getSchemaName(), this.schema.getTableName(), e);
        }
    }

    @SuppressWarnings("serial")
    public static class WriteRecordException extends RuntimeException {
        public WriteRecordException(String schemaName, String tableName, WriteFieldException e) {
            super(String.format("Can't write value: '%s' in '%s.%s(%s)'", e.getValue(), schemaName, tableName, e.getColumName()), e);
        }
    }
}
