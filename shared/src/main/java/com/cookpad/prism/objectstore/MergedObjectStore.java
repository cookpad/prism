package com.cookpad.prism.objectstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;

public interface MergedObjectStore {
    public InputStream getMergedObject(LocalDate dt, long lowerBound, long upperBound);
    public File getMergedObjectFile(LocalDate dt, long lowerBound, long upperBound) throws IOException;
    public String putMergedObjectFile(LocalDate dt, long lowerBound, long upperBound, File content);
    public String putMergedPartitionManifest(LocalDate dt, long manifestVersion, String content);
}
