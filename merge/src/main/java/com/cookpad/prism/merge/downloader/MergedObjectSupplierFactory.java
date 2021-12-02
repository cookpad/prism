package com.cookpad.prism.merge.downloader;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;

import com.cookpad.prism.merge.downloader.DownloadedObjectSupplier.ObjectDownloader;
import com.cookpad.prism.objectstore.MergedObjectStore;
import com.cookpad.prism.dao.PrismMergeRange;
import com.cookpad.prism.dao.PrismPartition;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MergedObjectSupplierFactory {
    private final MergedObjectStore mergedObjectStore;

    public DownloadedObjectSupplier createSupplier(PrismMergeRange mergeRange, PrismPartition partition) {
        return new DownloadedObjectSupplier(new MergedObjectDownloader(partition.getPartitionDate(), mergeRange.getLowerBound(), mergeRange.getUpperBound(), this.mergedObjectStore));
    }

    @RequiredArgsConstructor
    public static class MergedObjectDownloader implements ObjectDownloader {
        private final LocalDate dt;
        private final long lowerBound;
        private final long upperBound;
        private final MergedObjectStore mergedObjectStore;

        @Override
        public File download() throws IOException {
            return this.mergedObjectStore.getMergedObjectFile(this.dt, this.lowerBound, this.upperBound);
        }
    }
}
