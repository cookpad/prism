package com.cookpad.prism.batch;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
public class ListStagingObjectsCmd {
    private final S3Client s3;

    public void run(URI destS3Uri, String bucketName, String keyStartx, String keyEndx) throws IOException {
        Path tmpPath = Files.createTempFile("prism-list-staging-objects-", ".txt").toAbsolutePath();
        log.info("tmp file: {}", tmpPath.toString());
        try (PrintWriter outputWriter = new PrintWriter(new FileWriter(tmpPath.toFile(), StandardCharsets.UTF_8))) {
            String continuationToken = null;
            pagination: do {
                ListObjectsV2Request req = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .continuationToken(continuationToken)
                    .startAfter(keyStartx)
                    .build();
                ListObjectsV2Response res = this.s3.listObjectsV2(req);
                for (S3Object objectSummary : res.contents()) {
                    String key = objectSummary.key();
                    if (key.compareTo(keyEndx) >= 0) {
                        break pagination;
                    }
                    outputWriter.printf("s3://%s/%s%n", bucketName, key);
                }
                continuationToken = res.nextContinuationToken();
            } while(continuationToken != null);
        }
        String destBucket = destS3Uri.getHost();
        String destKey = destS3Uri.getPath().replaceFirst("^/", "");
        this.s3.putObject(
            PutObjectRequest.builder().bucket(destBucket).key(destKey).build(),
            RequestBody.fromFile(tmpPath)
        );
    }
}
