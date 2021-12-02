package com.cookpad.prism.batch;

import java.time.LocalDate;
import java.util.List;

import com.cookpad.prism.objectstore.PartitionManifest;
import com.cookpad.prism.objectstore.PrismTableLocator;
import com.cookpad.prism.dao.PrismMergeRange;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class ManifestGenerator {
    public PartitionManifest generate(PrismTableLocator tableLocator, LocalDate partitionDate, List<PrismMergeRange> mergeRanges) {
        PartitionManifest manifest = new PartitionManifest();
        for (PrismMergeRange mergeRange : mergeRanges) {
            String key = tableLocator.getMergedObjectKey(partitionDate, mergeRange.getLowerBound(), mergeRange.getUpperBound());
            String url = tableLocator.toFullUrl(key).toString();
            manifest.add(url, mergeRange.getContentLength());
        }
        return manifest;
    }
}
