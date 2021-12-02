package com.cookpad.prism.stream.filequeue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class FileQueue {
    private final LineNumberReader inner;
    private String currentLine = null;
    public FileQueue(InputStream in) {
        InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
        this.inner = new LineNumberReader(isr);
    }

    static FileQueue fromGzipStream(InputStream gzipped) throws IOException {
        GZIPInputStream unzipped = new GZIPInputStream(gzipped);
        return new FileQueue(unzipped);
    }

    static FileQueue fromGzipFile(File file) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(file);
        return FileQueue.fromGzipStream(fileInputStream);
    }

    static FileQueue fromPlainFile(File file) throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(file);
        return new FileQueue(fileInputStream);
    }

    public String peek() throws IOException {
        if (this.currentLine == null) {
            this.dequeue();
        }
        return this.currentLine;
    }

    public int lineNumber() {
        return this.inner.getLineNumber();
    }

    public String dequeue() throws IOException {
        String ret = this.currentLine;
        this.currentLine = this.inner.readLine();
        return ret;
    }
}
