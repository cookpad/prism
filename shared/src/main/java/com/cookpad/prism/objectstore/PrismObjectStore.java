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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class PrismObjectStore implements SmallObjectStore, MergedObjectStore {
    final private AmazonS3 s3;
    final private PrismTableLocator locator;

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
        s3.putObject(locator.getBucketName(), key, content);
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
        s3.putObject(locator.getBucketName(), key, content);
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
        s3.putObject(locator.getBucketName(), key, content);
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
