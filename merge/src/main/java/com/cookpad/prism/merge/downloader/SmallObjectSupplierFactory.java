package com.cookpad.prism.merge.downloader;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.cookpad.prism.merge.downloader.DownloadedObjectSupplier.ObjectDownloader;
import com.cookpad.prism.objectstore.SmallObjectStore;
import com.cookpad.prism.dao.PrismPartition;
import com.cookpad.prism.dao.PrismSmallObject;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SmallObjectSupplierFactory {
    private final SmallObjectStore smallObjectStore;

    public DownloadedObjectSupplier createSingleSupplier(PrismSmallObject smallObject, PrismPartition partition) throws IOException {
        LocalDate dt = partition.getPartitionDate();
        long objectId = smallObject.getStagingObjectId();
        if (smallObject.isDelayed()) {
            return new DownloadedObjectSupplier(new DelayedObjectDownloader(dt, objectId, this.smallObjectStore));
        } else {
            return new DownloadedObjectSupplier(new LiveObjectDownloader(dt, objectId, this.smallObjectStore));
        }
    }

    public List<DownloadedObjectSupplier> createMultipleSuppliers(List<PrismSmallObject> smallObjects, PrismPartition partition) throws IOException {
        List<DownloadedObjectSupplier> suppliers = new ArrayList<>();
        for (PrismSmallObject smallObject : smallObjects) {
            DownloadedObjectSupplier supplier = this.createSingleSupplier(smallObject, partition);
            suppliers.add(supplier);
        }
        return suppliers;
    }

    @RequiredArgsConstructor
    public static class LiveObjectDownloader implements ObjectDownloader {
        private final LocalDate dt;
        private final long objectId;
        private final SmallObjectStore smallObjectStore;

        @Override
        public File download() throws IOException {
            return this.smallObjectStore.getLiveObjectFile(this.dt, this.objectId);
        }
    }

    @RequiredArgsConstructor
    public static class DelayedObjectDownloader implements ObjectDownloader {
        private final LocalDate dt;
        private final long objectId;
        private final SmallObjectStore smallObjectStore;

        @Override
        public File download() throws IOException {
            return this.smallObjectStore.getDelayedObjectFile(this.dt, this.objectId);
        }
    }
}
