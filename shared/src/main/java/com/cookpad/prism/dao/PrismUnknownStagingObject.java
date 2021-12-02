package com.cookpad.prism.dao;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class PrismUnknownStagingObject {
    private static long UNINITIALIZED_ID = -1;

    private long id = UNINITIALIZED_ID;
    private String bucketName;
    private String objectKey;
    private LocalDateTime sendTime;
    private LocalDateTime firstReceiveTime;
    private String message;

    public long getId() {
        if (this.id == UNINITIALIZED_ID) {
            throw new IllegalStateException("Get uninitialized id of PrismStagingObject");
        }
        return this.id;
    }

    public URI getObjectUri() {
        try {
            return new URI("s3", this.getBucketName(), "/" + this.getObjectKey(), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
