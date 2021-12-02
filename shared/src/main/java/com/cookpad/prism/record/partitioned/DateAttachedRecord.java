package com.cookpad.prism.record.partitioned;

import java.time.LocalDate;

import org.apache.parquet.io.api.RecordConsumer;
import com.cookpad.prism.record.Record;
import com.cookpad.prism.record.partitioned.PartitionedRecord;
import com.cookpad.prism.record.values.Value;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DateAttachedRecord implements PartitionedRecord {
    final private Record inner;
    final private LocalDate dt;

    @Override
    public void writeMessage(RecordConsumer consumer) {
        this.inner.writeMessage(consumer);
    }

    @Override
    public LocalDate getPartitionDate() {
        return this.dt;
    }

    @Override
    public Value getValue(int index) {
        return this.inner.getValue(index);
    }
}
