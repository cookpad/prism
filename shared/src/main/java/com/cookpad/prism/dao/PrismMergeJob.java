package com.cookpad.prism.dao;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrismMergeJob {
    private long id;
    private long partitionId;
    private LocalDateTime scheduleTime;
    private long ongoingMark; // 0 if it's pending, same as id if it's ongoing
    private LocalDateTime heartbeatTime;
}
