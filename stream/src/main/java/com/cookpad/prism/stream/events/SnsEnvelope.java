package com.cookpad.prism.stream.events;

import java.io.IOException;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SnsEnvelope {
    @JsonProperty("Message")
    private String message;
    @JsonProperty("Timestamp")
    private Instant timestamp;

    private static ObjectMapper MAPPER = new ObjectMapper();
    static {{
        MAPPER.registerModule(new JavaTimeModule());
    }};
    public static SnsEnvelope parseJson(String json) throws IOException {
        return MAPPER.readValue(json, SnsEnvelope.class);
    }
}
