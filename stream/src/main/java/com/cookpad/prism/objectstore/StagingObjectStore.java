package com.cookpad.prism.objectstore;

import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;

import com.cookpad.prism.dao.PrismStagingObject;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StagingObjectStore {
    final private AmazonS3 s3;

    public InputStream getStagingObject(PrismStagingObject stagingObject) {
        S3Object s3Object = this.s3.getObject(stagingObject.getBucketName(), stagingObject.getObjectKey());
        return s3Object.getObjectContent();
    }
}
