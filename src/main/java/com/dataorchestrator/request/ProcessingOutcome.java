package com.dataorchestrator.request;

import com.dataorchestrator.job.JobEntity;

/**
 * Result of processing one Kafka request: the resulting job and whether the event was a
 * duplicate (already processed). Duplicates are acknowledged but do not create a new job.
 */
public record ProcessingOutcome(JobEntity job, boolean duplicate) {
}
