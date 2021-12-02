package com.cookpad.prism.dao;

import java.time.Instant;
import java.time.ZoneOffset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StreamColumn {
    private long id;
    private String name;
    private String sourceName;
    private String type;
    private Integer length;
    private String sourceOffset;
    private String zoneOffset;
    private Instant createTime;
    private boolean isPartitionSource;

    public ZoneOffset getZoneOffsetAsZoneOffset() {
        if (this.getZoneOffset() == null) {
            return null;
        }
        return ZoneOffset.of(this.getZoneOffset());
    }
}
