package com.cookpad.prism.merge;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import com.cookpad.prism.dao.PrismMergeJob;
import com.cookpad.prism.dao.PrismMergeJobMapper;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class MergeJobQueue {
    private final PrismMergeConf prismConf;
    private final PrismMergeJobMapper mergeJobMapper;
    private final Clock clock;

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(this.clock.instant(), ZoneOffset.UTC);
    }

    private LocalDateTime getTimedoutPeriod() {
        return this.now().minusSeconds(this.prismConf.getMergeJobTimeout());
    }

    public PrismMergeJob dequeue() {
        this.retryTimedoutJobs();
        return this.mergeJobMapper.dequeue(this.now());
    }

    public void retry(PrismMergeJob job) {
        // MEMO: transactions are not needed here
        //       because miss-deleted records will be deleted in next check
        this.mergeJobMapper.retry(job.getPartitionId(), job.getScheduleTime());
        this.mergeJobMapper.delete(job.getId());
    }

    public void retryTimedoutJobs() {
        List<PrismMergeJob> timedoutJobs = this.mergeJobMapper.findTimedoutJobs(this.getTimedoutPeriod(), 100);
        for (PrismMergeJob job : timedoutJobs) {
            this.retry(job);
        }
    }

    public void delete(PrismMergeJob job) {
        this.mergeJobMapper.delete(job.getId());
    }
}
