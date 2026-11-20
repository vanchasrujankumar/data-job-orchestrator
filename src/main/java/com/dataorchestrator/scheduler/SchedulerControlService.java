package com.dataorchestrator.scheduler;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dataorchestrator.config.OrchestratorProperties;

/**
 * Pause/resume control and status reporting for the dispatcher. The pause flag is persisted
 * in the database so it takes effect across every replica.
 */
@Service
public class SchedulerControlService {

    private final SchedulerStateRepository repository;
    private final ConcurrencyService concurrencyService;
    private final OrchestratorProperties props;

    public SchedulerControlService(SchedulerStateRepository repository,
                                   ConcurrencyService concurrencyService,
                                   OrchestratorProperties props) {
        this.repository = repository;
        this.concurrencyService = concurrencyService;
        this.props = props;
    }

    @Transactional
    public SchedulerStatus pause() {
        SchedulerStateEntity state = getOrCreate();
        state.setPaused(true);
        state.setPausedAt(Instant.now());
        repository.save(state);
        return status();
    }

    @Transactional
    public SchedulerStatus resume() {
        SchedulerStateEntity state = getOrCreate();
        state.setPaused(false);
        state.setResumedAt(Instant.now());
        repository.save(state);
        return status();
    }

    @Transactional(readOnly = true)
    public boolean isPaused() {
        return repository.findById(SchedulerStateEntity.SINGLETON_ID)
            .map(SchedulerStateEntity::isPaused)
            .orElse(false);
    }

    @Transactional(readOnly = true)
    public SchedulerStatus status() {
        Map<String, SchedulerStatus.TypeUtilization> byType = new LinkedHashMap<>();
        props.concurrency().jobTypes().keySet().forEach(type -> byType.put(type,
            new SchedulerStatus.TypeUtilization(
                props.concurrencyLimit(type),
                concurrencyService.reservedSlots(type),
                concurrencyService.queuedJobs(type))));
        return new SchedulerStatus(isPaused(), props.concurrency().globalMaxJobs(),
            concurrencyService.reservedSlotsGlobal(), byType);
    }

    private SchedulerStateEntity getOrCreate() {
        return repository.findById(SchedulerStateEntity.SINGLETON_ID)
            .orElseGet(() -> SchedulerStateEntity.builder()
                .id(SchedulerStateEntity.SINGLETON_ID)
                .paused(false)
                .build());
    }
}
