package com.cookpad.prism.jsonl;

import java.io.IOException;
import java.io.LineNumberReader;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;

public class JsonlReader implements AutoCloseable {
    private static final MappingJsonFactory FACTORY = new MappingJsonFactory();

    final private LineNumberReader inner;
    public JsonlReader(LineNumberReader inner) {
        this.inner = inner;
    }

    public JsonNode read() throws IOException {
        String line = this.inner.readLine();
        if (line == null) {
            return null;
        }
        JsonParser parser = FACTORY.createParser(line);
        return parser.readValueAs(JsonNode.class);
    }

    @Override
    public void close() throws IOException {
        this.inner.close();
    }
}
