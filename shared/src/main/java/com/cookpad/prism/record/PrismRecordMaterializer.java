package com.cookpad.prism.record;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;

import lombok.NonNull;

public class PrismRecordMaterializer extends RecordMaterializer<Record> {
    public final RecordConverter root;

    public PrismRecordMaterializer(@NonNull Schema schema) {
        this.root = new RecordConverter(schema);
    }

    @Override
    public Record getCurrentRecord() {
        return this.root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return this.root;
    }
}
