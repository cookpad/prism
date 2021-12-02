package com.cookpad.prism.merge;

import com.cookpad.prism.dao.PrismMergeJob;

public interface MergeJobHandler {
    public JobStatus handleJob(PrismMergeJob job) throws Exception;

    public static enum JobStatus {
        FINISHED,
        CONTINUING,
    }

    public void shutdown();
}
