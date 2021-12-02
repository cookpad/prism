package com.cookpad.prism.dao;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.ibatis.annotations.Param;

public interface PrismMergeRangeMapper {
    PrismMergeRange findOpenRange(@Param("partitionId") long partitionId);
    List<PrismMergeRange> findAllInPartition(@Param("partitionId") long partitionId);
    void upsertRange(
        @Param("partitionId") long partitionId,
        @Param("lowerBound") long lowerBound,
        @Param("upperBound") long upperBound,
        @Param("contentLength") long contentLength,
        @Param("now") LocalDateTime now
    );
}
