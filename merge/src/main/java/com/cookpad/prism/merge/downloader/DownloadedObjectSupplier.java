package com.cookpad.prism.merge.downloader;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import com.cookpad.prism.TempFile;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DownloadedObjectSupplier implements Supplier<TempFile> {
    private final ObjectDownloader downloader;

    @Override
    public TempFile get() {
        try {
            File file = this.downloader.download();
            return new TempFile(file.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static interface ObjectDownloader {
        File download() throws IOException;
    }
}
