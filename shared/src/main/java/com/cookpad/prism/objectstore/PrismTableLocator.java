package com.cookpad.prism.objectstore;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PrismTableLocator {
    @Getter
    final private String bucketName;
    @Getter
    final private String tablePrefix;

    private static final DateTimeFormatter DT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public String formatDt(LocalDate dt) {
        return dt.format(DT_FORMAT);
    }

    private String getSmallObjectPartitionPrefix(String type, LocalDate dt) {
        String yyyyMMdd = this.formatDt(dt);
        String key = String.format("%s%s/dt=%s/", this.tablePrefix, type, yyyyMMdd);
        return key;
    }

    private String getSmallObjectBasename(long objectId) {
        return String.format("prism-%019d.parquet", objectId);
    }

    public String getLiveObjectPartitionPrefix(LocalDate dt) {
        return this.getSmallObjectPartitionPrefix("live", dt);
    }

    public String getDelayedObjectPartitionPrefix(LocalDate dt) {
        return this.getSmallObjectPartitionPrefix("delayed", dt);
    }

    public String getLiveObjectKey(LocalDate dt, long objectId) {
        String prefix = this.getLiveObjectPartitionPrefix(dt);
        String basename = this.getSmallObjectBasename(objectId);
        return prefix + basename;
    }

    public String getDelayedObjectKey(LocalDate dt, long objectId) {
        String prefix = this.getDelayedObjectPartitionPrefix(dt);
        String basename = this.getSmallObjectBasename(objectId);
        return prefix + basename;
    }

    public String getMergedObjectPartitionPrefix(LocalDate dt) {
        String yyyyMMdd = this.formatDt(dt);
        String prefix = String.format("%smerged/dt=%s/", this.tablePrefix, yyyyMMdd);
        return prefix;
    }

    public String getMergedObjectKey(LocalDate dt, long lowerBound, long upperBound) {
        String prefix = this.getMergedObjectPartitionPrefix(dt);
        String key = String.format("%spart-%019d-%019d.parquet", prefix, lowerBound, upperBound);
        return key;
    }

    public String getMergedPartitionManifestKey(LocalDate dt, long manifestVersion) {
        String prefix = this.getMergedObjectPartitionPrefix(dt);
        String key = String.format("%smanifest-%019d.json", prefix, manifestVersion);
        return key;
    }

    public URI toFullUrl(String key) {
        try {
            return new URI("s3", this.getBucketName(), "/" + key, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
