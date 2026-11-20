package com.dataorchestrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Data Job Orchestration Service.
 *
 * <p>This service consumes Dataproc job execution requests from Kafka, persists and
 * validates them, dispatches them to Google Cloud Dataproc under configurable concurrency
 * limits, tracks their lifecycle, retries transient failures, alerts on permanent failures,
 * and publishes completion/failure events back to Kafka.</p>
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class DataJobOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataJobOrchestratorApplication.class, args);
    }
}
