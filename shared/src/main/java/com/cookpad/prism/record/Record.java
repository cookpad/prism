package com.cookpad.prism.record;

import org.apache.parquet.io.api.RecordConsumer;
import com.cookpad.prism.record.values.Value;

public interface Record {
    public void writeMessage(RecordConsumer consumer);
    public Value getValue(int index);
}
