package com.cookpad.prism.record.values;

import org.apache.parquet.io.api.RecordConsumer;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class NullValue implements Value {
    @Override
    public void writeField(RecordConsumer consumer) {
        // Do nothing
        return;
    }
}
