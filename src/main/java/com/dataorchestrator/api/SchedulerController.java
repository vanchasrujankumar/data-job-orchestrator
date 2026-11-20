package com.dataorchestrator.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataorchestrator.scheduler.SchedulerControlService;
import com.dataorchestrator.scheduler.SchedulerStatus;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * Scheduler control endpoints: status, pause and resume. Pause/resume is persisted and
 * therefore applies across all replicas.
 */
@RestController
@RequestMapping("/api/v1/scheduler")
@Tag(name = "Scheduler", description = "Scheduler status and pause/resume control")
public class SchedulerController {

    private final SchedulerControlService schedulerControlService;

    public SchedulerController(SchedulerControlService schedulerControlService) {
        this.schedulerControlService = schedulerControlService;
    }

    @GetMapping("/status")
    @Operation(summary = "Scheduler status", description = "Pause state and concurrency utilisation.")
    public SchedulerStatus status() {
        return schedulerControlService.status();
    }

    @PostMapping("/pause")
    @Operation(summary = "Pause dispatching", description = "Stop new job dispatches across replicas.")
    public SchedulerStatus pause() {
        return schedulerControlService.pause();
    }

    @PostMapping("/resume")
    @Operation(summary = "Resume dispatching", description = "Resume job dispatching across replicas.")
    public SchedulerStatus resume() {
        return schedulerControlService.resume();
    }
}
