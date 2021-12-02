package com.cookpad.prism.merge;

import com.cookpad.prism.StepHandler;
import com.cookpad.prism.merge.MergeJobHandler.JobStatus;
import com.cookpad.prism.dao.PrismMergeJob;
import org.springframework.stereotype.Component;

import io.sentry.Sentry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
@Component
public class MergeJobDispatcher implements StepHandler {
    private final MergeJobQueue mergeJobQueue;
    private final MergeJobHandler mergeJobHandler;

    @Override
    public boolean handleStep() {
        PrismMergeJob job = this.mergeJobQueue.dequeue();
        if (job == null) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                return true;
            }
            return true;
        }
        try {
            log.info("Handling job: {}", job);
            Sentry.getContext().addTag("merge_job", Long.toString(job.getId()));
            JobStatus status = this.mergeJobHandler.handleJob(job);
            if (status == JobStatus.CONTINUING) {
                this.mergeJobQueue.retry(job);
            } else {
                this.mergeJobQueue.delete(job);
            }
            log.info("Handled job: {}", job);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            Sentry.getContext().removeTag("merge_job");
        }
        return true;
    }

    public void shutdown() {
        mergeJobHandler.shutdown();
    }
}
