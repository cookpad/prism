package com.cookpad.prism.dao;

import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrismPartition {
    private long id;
    private int tableId;
    private LocalDate partitionDate;
    private long currentManifestVersion;
    private long desiredManifestVersion;
    private Long lastLiveObjectId;
    private boolean switched;
}
