package com.cookpad.prism.objectstore;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class PrismObjectStoreTest {
    private S3Client s3;
    private PrismTableLocator locator;
    private PrismObjectStore store;

    @BeforeEach
    void setUp() {
        s3 = mock(S3Client.class);
        locator = mock(PrismTableLocator.class);
        store = new PrismObjectStore(s3, locator);
    }

    @Test
    void putLiveObjectFile_shouldCreateRequestWithCorrectTag() throws IOException {
        // Arrange
        LocalDate testDate = LocalDate.of(2025, 1, 1);
        long objectId = 100;
        File testFile = Files.createTempFile("prism-test-", ".tmp").toFile();
        String bucketName = "test-bucket";
        String testKey = "test-key";

        when(locator.getBucketName()).thenReturn(bucketName);
        when(locator.getLiveObjectKey(testDate, objectId)).thenReturn(testKey);

        // Act
        store.putLiveObjectFile(testDate, objectId, testFile);

        // Assert
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3).putObject(requestCaptor.capture(), any(RequestBody.class));

        PutObjectRequest capturedRequest = requestCaptor.getValue();
        assertEquals(bucketName, capturedRequest.bucket());
        assertEquals(testKey, capturedRequest.key());

        assertEquals("PrismObjectType=live", capturedRequest.tagging());
    }
}
