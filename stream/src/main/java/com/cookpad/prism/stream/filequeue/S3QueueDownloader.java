package com.cookpad.prism.stream.filequeue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class S3QueueDownloader {
    private final AmazonS3 s3;

    public FileQueue download(AmazonS3URI queueObjectUrl) throws IOException {
        String key = queueObjectUrl.getKey();
        S3Object queueObject = s3.getObject(queueObjectUrl.getBucket(), key);
        Path tmpPath = Files.createTempFile("prism-rebuild-queue-", ".queue").toAbsolutePath();
        try (InputStream in = queueObject.getObjectContent()) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        if (key.endsWith(".gz")) {
            return FileQueue.fromGzipFile(tmpPath.toFile());
        } else {
            return FileQueue.fromPlainFile(tmpPath.toFile());
        }
    }
}
