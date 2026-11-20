package com.dataorchestrator.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for the MongoDB repository, run against a real MongoDB in a
 * Testcontainers container. Automatically skipped when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@ActiveProfiles("test")
class JobRepositoryIntegrationTest {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri",
            () -> "mongodb://" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017) + "/orchestrator");
    }

    @Autowired
    private JobRepository jobRepository;

    @Test
    void saveAndFindByJobId() {
        JobEntity job = JobEntity.builder()
            .jobId("job-1")
            .eventId("evt-1")
            .jobType("SEGMENT_PREVIEW")
            .priority(JobPriority.NORMAL)
            .status(JobStatus.QUEUED)
            .attemptCount(0)
            .maxAttempts(3)
            .executionTimeoutSeconds(3600)
            .receivedAt(Instant.now())
            .build();

        jobRepository.save(job);

        assertThat(jobRepository.findByJobId("job-1")).isPresent();
        assertThat(jobRepository.existsByEventId("evt-1")).isTrue();
        assertThat(jobRepository.countByStatusIn(java.util.List.of(JobStatus.QUEUED))).isEqualTo(1);
    }

    @Test
    void findByStatusInReturnsMatchingJobs() {
        JobEntity queued = JobEntity.builder()
            .jobId("job-2").eventId("evt-2").jobType("SEGMENT_PREVIEW")
            .priority(JobPriority.NORMAL).status(JobStatus.QUEUED)
            .attemptCount(0).maxAttempts(3).executionTimeoutSeconds(3600)
            .receivedAt(Instant.now()).build();
        jobRepository.save(queued);

        assertThat(jobRepository.findByStatusIn(java.util.List.of(JobStatus.QUEUED)))
            .extracting(JobEntity::getJobId)
            .contains("job-2");
    }
}
