package com.cookpad.prism.merge.downloader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.cookpad.prism.objectstore.SmallObjectStore;
import com.cookpad.prism.dao.PrismPartition;
import com.cookpad.prism.dao.PrismSmallObject;

import lombok.val;

public class SmallObjectSupplierFactoryTest {
    @Test
    public void testCreateSingleSupplierDelayed() throws IOException {
        val dt = LocalDate.of(2018, 9, 5);
        val objectId = 100;

        val path = mock(Path.class);
        val file = mock(File.class);
        when(file.toPath())
            .thenReturn(path);
        val smallObjectStore = mock(SmallObjectStore.class);
        when(smallObjectStore.getDelayedObjectFile(dt, objectId))
            .thenReturn(file);
        //smallObjectStore.
        val downloader = new SmallObjectSupplierFactory(smallObjectStore);
        val supplier = downloader.createSingleSupplier(
            new PrismSmallObject(1, objectId, 2, true, 1000, LocalDateTime.now()),
            new PrismPartition(2, 3, dt, -1, 0, null, false)
        );
        assertEquals(path, supplier.get().getPath());
    }

    @Test
    public void testCreateSingleSupplierLive() throws IOException {
        val dt = LocalDate.of(2018, 9, 5);
        val objectId = 100;

        val path = mock(Path.class);
        val file = mock(File.class);
        when(file.toPath())
            .thenReturn(path);
        val smallObjectStore = mock(SmallObjectStore.class);
        when(smallObjectStore.getLiveObjectFile(dt, objectId))
            .thenReturn(file);
        //smallObjectStore.
        val downloader = new SmallObjectSupplierFactory(smallObjectStore);
        val supplier = downloader.createSingleSupplier(
            new PrismSmallObject(1, objectId, 2, false, 1000, LocalDateTime.now()),
            new PrismPartition(2, 3, dt, -1, 0, null, false)
        );
        assertEquals(path, supplier.get().getPath());
    }
}
