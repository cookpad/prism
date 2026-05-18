package com.cookpad.prism.stream.filequeue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class S3QueueDownloader {
    private final S3Client s3;

    public FileQueue download(URI queueObjectUrl) throws IOException {
        String bucket = queueObjectUrl.getHost();
        String key = queueObjectUrl.getPath().replaceFirst("^/", "");
        InputStream queueObject = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
        Path tmpPath = Files.createTempFile("prism-rebuild-queue-", ".queue").toAbsolutePath();
        try (InputStream in = queueObject) {
            Files.copy(in, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        }
        if (key.endsWith(".gz")) {
            return FileQueue.fromGzipFile(tmpPath.toFile());
        } else {
            return FileQueue.fromPlainFile(tmpPath.toFile());
        }
    }
}
