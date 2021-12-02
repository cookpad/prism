package com.cookpad.prism.record.partitioned;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;

import com.cookpad.prism.TempFile;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PartitionCollector implements AutoCloseable {
    private TreeMap<LocalDate, TempFile> partitionToTempFile = null;

    public TreeMap<LocalDate, Path> collect() {
        if (this.partitionToTempFile == null) {
            throw new IllegalStateException("Close PartitionedWriter before collect partitions");
        }
        TreeMap<LocalDate, Path> result = new TreeMap<>();
        for (Map.Entry<LocalDate, TempFile> kv : this.partitionToTempFile.entrySet()) {
            result.put(kv.getKey(), kv.getValue().getPath());
        }
        return result;
    }

    public void commit(TreeMap<LocalDate, TempFile> partitionToTempFile) {
        if (this.partitionToTempFile != null) {
            throw new IllegalStateException("Multiple commit is not allowed");
        }
        this.partitionToTempFile = partitionToTempFile;
    }

    // Clean up temp files or they will run out of disk space
    @Override
    public void close() throws IOException {
        for (TempFile dest: this.partitionToTempFile.values()) {
            File file = dest.getPath().toFile();
            if (!file.delete()) {
                log.error("Failed to delete temp file: {}", file.getPath());
            }
            log.debug("Deleted temp file: {}", file.getPath());
        }
    }
}
