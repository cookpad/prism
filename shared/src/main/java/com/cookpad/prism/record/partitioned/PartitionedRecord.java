package com.cookpad.prism.record.partitioned;

import java.time.LocalDate;

import com.cookpad.prism.record.Record;

public interface PartitionedRecord extends Record {
    public abstract LocalDate getPartitionDate();
}
