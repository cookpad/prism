package com.cookpad.prism.batch;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
public class ListStagingObjectsCmd {
    private final AmazonS3 s3;

    public void run(AmazonS3URI destS3Uri, String bucketName, String keyStartx, String keyEndx) throws IOException {
        Path tmpPath = Files.createTempFile("prism-list-staging-objects-", ".txt").toAbsolutePath();
        log.info("tmp file: {}", tmpPath.toString());
        try (PrintWriter outputWriter = new PrintWriter(new FileWriter(tmpPath.toFile(), StandardCharsets.UTF_8))) {
            String continuationToken = null;
            pagination: do {
                ListObjectsV2Request req = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withContinuationToken(continuationToken)
                    .withStartAfter(keyStartx);
                ListObjectsV2Result res = this.s3.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : res.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    if (key.compareTo(keyEndx) >= 0) {
                        break pagination;
                    }
                    outputWriter.printf("s3://%s/%s%n", objectSummary.getBucketName(), key);
                }
                continuationToken = res.getNextContinuationToken();
            } while(continuationToken != null);
        }
        PutObjectRequest putObjectRequest = new PutObjectRequest(destS3Uri.getBucket(), destS3Uri.getKey(), tmpPath.toFile());
        this.s3.putObject(putObjectRequest);
    }
}
