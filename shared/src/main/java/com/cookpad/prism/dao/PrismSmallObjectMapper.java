package com.cookpad.prism.dao;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface PrismSmallObjectMapper {
    PrismSmallObject findByParams(@Param("stagingObjectId") long stagingObjectId, @Param("partitionId") long partitionId);
    PrismSmallObject createByParams(@Param("stagingObjectId") long stagingObjectId, @Param("partitionId") long partitionId, @Param("uploadStartTime") LocalDateTime uploadStartTime, @Param("contentLength") long contentLength);
    List<PrismSmallObject> findNewObjects(@Param("partitionId") long partitionId, @Param("lowerBound") long lowerBound, @Param("limit") int limit);
    List<PrismSmallObject> findAllObjectsInRange(@Param("partitionId") long partitionId, @Param("lowerBound") long lowerBound, @Param("upperBound") long upperBound);

    @Transactional(propagation = Propagation.NESTED)
    default PrismSmallObject findOrCreateByParams(long stagingObjectId, long partitionId, LocalDateTime uploadStartTime, long contentLength) {
        PrismSmallObject smallObject = this.findByParams(stagingObjectId, partitionId);
        if (smallObject != null) {
            return smallObject;
        }
        smallObject = this.createByParams(stagingObjectId, partitionId, uploadStartTime, contentLength);
        return smallObject;
    }
}
