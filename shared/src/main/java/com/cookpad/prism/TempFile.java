package com.cookpad.prism;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TempFile implements AutoCloseable {
    @Getter
    private final Path path;

    public TempFile(String prefix, String suffix) throws IOException {
        this.path = Files.createTempFile(prefix, suffix);
        log.debug("Created temp file: {}", this.path);
    }

    @Override
    public void close() throws IOException {
        File file = this.path.toFile();
        if (!file.delete()) {
            log.warn("Failed to delete temp file: {}", file.getPath());
        }
        log.debug("Deleted temp file: {}", file.getPath());
    }

    @RequiredArgsConstructor
    public static class Factory {
        private final String prefix;
        private final String suffix;

        public TempFile create() throws IOException {
            return new TempFile(this.prefix, this.suffix);
        }
    }
}
