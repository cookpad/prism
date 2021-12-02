package com.cookpad.prism.stream.events;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import com.cookpad.prism.dao.PrismStagingObject;
import com.cookpad.prism.dao.PrismUnknownStagingObject;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class StagingObjectEvent {
    final private String bucketName;
    final private String objectKey;
    final private Instant sendTime;
    final private Instant receiveTime;

    public PrismStagingObject toStagingObject() {
        PrismStagingObject stagingObject = new PrismStagingObject();
        stagingObject.setBucketName(bucketName);
        stagingObject.setObjectKey(objectKey);
        stagingObject.setSendTime(LocalDateTime.ofInstant(this.sendTime, ZoneOffset.UTC));
        stagingObject.setFirstReceiveTime(LocalDateTime.ofInstant(this.receiveTime, ZoneOffset.UTC));
        return stagingObject;
    }

    public PrismUnknownStagingObject toUnknownStagingObject(String message) {
        PrismUnknownStagingObject stagingObject = new PrismUnknownStagingObject();
        stagingObject.setBucketName(bucketName);
        stagingObject.setObjectKey(objectKey);
        stagingObject.setSendTime(LocalDateTime.ofInstant(this.sendTime, ZoneOffset.UTC));
        stagingObject.setFirstReceiveTime(LocalDateTime.ofInstant(this.receiveTime, ZoneOffset.UTC));
        stagingObject.setMessage(message);
        return stagingObject;
    }

    public URI getObjectUri() {
        try {
            return new URI("s3", this.getBucketName(), "/" + this.getObjectKey(), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
