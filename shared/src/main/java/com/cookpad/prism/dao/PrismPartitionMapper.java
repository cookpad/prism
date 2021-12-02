package com.cookpad.prism.dao;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.ibatis.annotations.Param;

public interface PrismPartitionMapper {
    PrismPartition createPartitionIfNotExists(@Param("tableId") int tableId, @Param("partitionDate") LocalDate partitionDate);
    PrismPartition find(@Param("id") long id);
    PrismPartition findByTableIdAndDate(@Param("tableId") int tableId, @Param("partitionDate") LocalDate partitionDate);
    void closePartitions(@Param("now") LocalDateTime now);
    void switchPartitions();
    List<PrismPartition> getNewPartitions();
    List<PrismPartition> getSwitchedPartitionsToUpdate();
    void updateCurrentManifestVersion(@Param("id") long id, @Param("currentManifestVersion") long currentManifestVersion);
    void updateDesiredManifestVersion(@Param("id") long id, @Param("desiredManifestVersion") long desiredManifestVersion);
}
