package com.dataorchestrator.scheduler;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * MongoDB singleton document (id = "singleton") holding the scheduler pause/resume flag.
 * Persisting this means a pause request takes effect across every replica of the service.
 */
@Document(collection = "scheduler_state")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchedulerStateEntity {

    public static final String SINGLETON_ID = "singleton";

    @Id
    private String id;

    private boolean paused;

    private Instant pausedAt;

    private Instant resumedAt;

    @LastModifiedDate
    private Instant updatedAt;
}
