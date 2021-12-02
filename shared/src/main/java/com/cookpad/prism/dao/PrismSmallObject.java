package com.cookpad.prism.dao;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class PrismSmallObject {
    private static long UNINITIALIZED_ID = -1;

    private long id = UNINITIALIZED_ID;
    private long stagingObjectId;
    private long partitionId;
    private boolean isDelayed;
    private long contentLength;
    private LocalDateTime uploadStartTime;

    public long getId() {
        if (this.id == UNINITIALIZED_ID) {
            throw new IllegalStateException("Get uninitialized id of PrismSmallObject");
        }
        return this.id;
    }
}
