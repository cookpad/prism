package com.cookpad.prism.dao;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.ibatis.annotations.Param;

public interface PrismMergeJobMapper {
    void enqueue(@Param("partitionId") long partitionId, @Param("scheduleTime") LocalDateTime scheduleTime);
    void retry(@Param("partitionId") long partitionId, @Param("scheduleTime") LocalDateTime scheduleTime);
    void delete(@Param("id") long id);
    PrismMergeJob dequeue(@Param("now") LocalDateTime now);
    List<PrismMergeJob> findTimedoutJobs(@Param("timedoutPeriod") LocalDateTime timedoutPeriod, @Param("limit") int limit);
}
