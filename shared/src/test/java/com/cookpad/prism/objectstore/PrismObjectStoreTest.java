package com.cookpad.prism.objectstore;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Tag;

public class PrismObjectStoreTest {
    private AmazonS3 s3;
    private PrismTableLocator locator;
    private PrismObjectStore store;

    @BeforeEach
    void setUp() {
        s3 = mock(AmazonS3.class);
        locator = mock(PrismTableLocator.class);
        store = new PrismObjectStore(s3, locator);
    }

    @Test
    void putLiveObjectFile_shouldCreateRequestWithCorrectTag() {
        // Arrange
        LocalDate testDate = LocalDate.of(2025, 1, 1);
        long objectId = 100;
        File testFile = mock(File.class);
        String bucketName = "test-bucket";
        String testKey = "test-key";

        when(locator.getBucketName()).thenReturn(bucketName);
        when(locator.getLiveObjectKey(testDate, objectId)).thenReturn(testKey);

        // Act
        store.putLiveObjectFile(testDate, objectId, testFile);

        // Assert
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3).putObject(requestCaptor.capture());

        PutObjectRequest capturedRequest = requestCaptor.getValue();
        assertEquals(bucketName, capturedRequest.getBucketName());
        assertEquals(testKey, capturedRequest.getKey());
        assertEquals(testFile, capturedRequest.getFile());

        ObjectTagging tagging = capturedRequest.getTagging();
        List<Tag> tags = tagging.getTagSet();
        assertEquals(1, tags.size());
        assertEquals("PrismObjectType", tags.get(0).getKey());
        assertEquals("live", tags.get(0).getValue());
    }
}
