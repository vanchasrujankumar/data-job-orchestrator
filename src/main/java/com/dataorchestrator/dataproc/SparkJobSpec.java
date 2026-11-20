package com.dataorchestrator.dataproc;

import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Spark workload specification passed through to Dataproc. Serialized as JSON into the
 * {@code job.execution_config} column and deserialized at submission time.
 */
public record SparkJobSpec(
    @NotBlank String mainClass,
    @NotNull List<String> jarFileUris,
    @NotNull List<String> args,
    @NotNull Map<String, String> properties,
    String clusterName
) {
}
