package com.cookpad.prism.objectstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class PrismObjectStore implements SmallObjectStore, MergedObjectStore {
    private static final String PRISM_OBJECT_TYPE_KEY = "PrismObjectType";

    final private S3Client s3;
    final private PrismTableLocator locator;

    private enum PrismObjectType {
        MERGED,
        LIVE,
        DELAYED;

        public String getTagValue() {
            return this.name().toLowerCase();
        }
    }

    private void putObjectWithTag(String bucketName, String key, File content, PrismObjectType prismObjectType) {
        PutObjectRequest request = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .tagging(PRISM_OBJECT_TYPE_KEY + "=" + prismObjectType.getTagValue())
            .build();
        s3.putObject(request, RequestBody.fromFile(content));
    }

    @Override
    public InputStream getLiveObject(LocalDate dt, long objectId) {
        String key = locator.getLiveObjectKey(dt, objectId);
        log.debug("Get live object: {}", key);
        return s3.getObject(GetObjectRequest.builder().bucket(locator.getBucketName()).key(key).build());
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
        putObjectWithTag(locator.getBucketName(), key, content, PrismObjectType.LIVE);
        return key;
    }

    @Override
    public InputStream getDelayedObject(LocalDate dt, long objectId) {
        String key = locator.getDelayedObjectKey(dt, objectId);
        log.debug("Get delayed object: {}", key);
        return s3.getObject(GetObjectRequest.builder().bucket(locator.getBucketName()).key(key).build());
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
        putObjectWithTag(locator.getBucketName(), key, content, PrismObjectType.DELAYED);
        return key;
    }

    @Override
    public InputStream getMergedObject(LocalDate dt, long lowerBound, long upperBound) {
        String key = locator.getMergedObjectKey(dt, lowerBound, upperBound);
        log.debug("Get merged object: {}", key);
        return s3.getObject(GetObjectRequest.builder().bucket(locator.getBucketName()).key(key).build());
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
        putObjectWithTag(locator.getBucketName(), key, content, PrismObjectType.MERGED);
        return key;
    }

    @Override
    public String putMergedPartitionManifest(LocalDate dt, long manifestVersion, String content) {
        String key = locator.getMergedPartitionManifestKey(dt, manifestVersion);
        log.info("Put partition manifest: {}", key);
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        log.info("PutObject (manifest) key={}", key);
        s3.putObject(
            PutObjectRequest.builder().bucket(locator.getBucketName()).key(key).build(),
            RequestBody.fromBytes(contentBytes)
        );
        return key;
    }
}
