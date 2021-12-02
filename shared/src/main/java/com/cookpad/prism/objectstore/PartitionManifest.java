package com.cookpad.prism.objectstore;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

public class PartitionManifest {
    @JsonProperty("entries")
    private List<Entry> entries = new ArrayList<>();

    @RequiredArgsConstructor
    public static class Entry {
        @JsonProperty("url")
        private final String url;
        @JsonProperty("meta")
        private final Meta meta;

        public Entry(String url, long contentLength) {
            this(url, new Meta(contentLength));
        }

        @RequiredArgsConstructor
        public static class Meta {
            @JsonProperty("content_length")
            private final long contentLength;
        }
    }

    public void add(String url, long contentLength) {
        this.entries.add(new Entry(url, contentLength));
    }

    public String toJSON() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
