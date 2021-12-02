package com.cookpad.prism.objectstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;

public interface SmallObjectStore {
    public InputStream getLiveObject(LocalDate dt, long objectId);
    public File getLiveObjectFile(LocalDate dt, long objectId) throws IOException;
    public String putLiveObjectFile(LocalDate dt, long objectId, File content);
    public InputStream getDelayedObject(LocalDate dt, long objectId);
    public File getDelayedObjectFile(LocalDate dt, long objectId) throws IOException;
    public String putDelayedObjectFile(LocalDate dt, long objectId, File content);
}
