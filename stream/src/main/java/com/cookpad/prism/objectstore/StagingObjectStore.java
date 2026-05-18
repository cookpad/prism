package com.cookpad.prism.objectstore;

import java.io.InputStream;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import com.cookpad.prism.dao.PrismStagingObject;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StagingObjectStore {
    final private S3Client s3;

    public InputStream getStagingObject(PrismStagingObject stagingObject) {
        return s3.getObject(GetObjectRequest.builder()
            .bucket(stagingObject.getBucketName())
            .key(stagingObject.getObjectKey())
            .build());
    }
}
