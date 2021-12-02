package com.cookpad.prism.record.partitioned;

import java.io.IOException;

public interface PartitionedRecordWriter extends AutoCloseable {
    public void write(PartitionedRecord record) throws IOException;
    @Override
    public void close() throws IOException;
}
