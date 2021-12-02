package com.cookpad.prism.dao;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrismMergeRange {
    private long id;
    private long partitionId;
    private long lowerBound;
    private long upperBound;
    private long contentLength;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
