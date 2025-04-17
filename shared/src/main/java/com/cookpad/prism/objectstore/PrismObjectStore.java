package com.cookpad.prism.objectstore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.Tag;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class PrismObjectStore implements SmallObjectStore, MergedObjectStore {
    private static final String PRISM_OBJECT_TYPE_KEY = "PrismObjectType";
    private static final String MERGED_OBJECT_TYPE = "merged";
    private static final String LIVE_OBJECT_TYPE = "live";
    private static final String DELAYED_OBJECT_TYPE = "delayed";

    final private AmazonS3 s3;
    final private PrismTableLocator locator;

    private PutObjectRequest createPutRequestWithTag(String bucketName, String key, File content, String tagValue) {
        PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, content);
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag(PRISM_OBJECT_TYPE_KEY, tagValue));
        putRequest.setTagging(new ObjectTagging(tags));
        return putRequest;
    }

    @Override
    public InputStream getLiveObject(LocalDate dt, long objectId) {
        String key = locator.getLiveObjectKey(dt, objectId);
        log.debug("Get live object: {}", key);
        S3Object object = s3.getObject(locator.getBucketName(), key);
        return object.getObjectContent();
    }

    @Override
    public File getLiveObjectFile(LocalDate dt, long objectId) throws IOException {
        Path tmpPath = Files.createTempFile("prism-batch-live-", ".parquet").toAbsolutePath();
        try (InputStream in = this.getLiveObject(dt, objectId)) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmpPath.toFile();
    }

    @Override
    public String putLiveObjectFile(LocalDate dt, long objectId, File content) {
        String key = locator.getLiveObjectKey(dt, objectId);
        log.info("PutObject (live) key={}", key);

        PutObjectRequest putRequest = createPutRequestWithTag(
            locator.getBucketName(), key, content, LIVE_OBJECT_TYPE);

        s3.putObject(putRequest);
        return key;
    }

    @Override
    public InputStream getDelayedObject(LocalDate dt, long objectId) {
        String key = locator.getDelayedObjectKey(dt, objectId);
        log.debug("Get delayed object: {}", key);
        S3Object object = s3.getObject(locator.getBucketName(), key);
        return object.getObjectContent();
    }

    @Override
    public File getDelayedObjectFile(LocalDate dt, long objectId) throws IOException {
        Path tmpPath = Files.createTempFile("prism-batch-delayed-", ".parquet").toAbsolutePath();
        try (InputStream in = this.getDelayedObject(dt, objectId)) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmpPath.toFile();
    }

    @Override
    public String putDelayedObjectFile(LocalDate dt, long objectId, File content) {
        String key = locator.getDelayedObjectKey(dt, objectId);
        log.info("PutObject (delayed) key={}", key);

        PutObjectRequest putRequest = createPutRequestWithTag(
            locator.getBucketName(), key, content, DELAYED_OBJECT_TYPE);

        s3.putObject(putRequest);
        return key;
    }

    @Override
    public InputStream getMergedObject(LocalDate dt, long lowerBound, long upperBound) {
        String key = locator.getMergedObjectKey(dt, lowerBound, upperBound);
        log.debug("Get merged object: {}", key);
        S3Object object = s3.getObject(locator.getBucketName(), key);
        return object.getObjectContent();
    }

    @Override
    public File getMergedObjectFile(LocalDate dt, long lowerBound, long upperBound) throws IOException {
        Path tmpPath = Files.createTempFile("prism-batch-merged-", ".parquet").toAbsolutePath();
        try (InputStream in = this.getMergedObject(dt, lowerBound, upperBound)) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return tmpPath.toFile();
    }

    @Override
    public String putMergedObjectFile(LocalDate dt, long lowerBound, long upperBound, File content) {
        String key = locator.getMergedObjectKey(dt, lowerBound, upperBound);
        log.info("PutObject (merged) key={}", key);

        PutObjectRequest putRequest = createPutRequestWithTag(
            locator.getBucketName(), key, content, MERGED_OBJECT_TYPE);

        s3.putObject(putRequest);
        return key;
    }

    @Override
    public String putMergedPartitionManifest(LocalDate dt, long manifestVersion, String content) {
        String key = locator.getMergedPartitionManifestKey(dt, manifestVersion);
        log.info("Put partition manifest: {}", key);
        ObjectMetadata metadata = new ObjectMetadata();
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        metadata.setContentLength(contentBytes.length);
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        log.info("PutObject (manifest) key={}", key);
        s3.putObject(locator.getBucketName(), key, contentStream, metadata);
        return key;
    }
}
