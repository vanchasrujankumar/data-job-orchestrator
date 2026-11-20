package com.dataorchestrator.dataproc;

import java.util.List;
import java.util.Map;

/**
 * Static definition of a Spark job for a given job type. The Spark workload itself
 * (main class, jar files and Spark configuration such as dynamic executor allocation and
 * memory settings) is configured once per job type; each Kafka request only supplies the
 * business parameters that are passed to the job as a JSON argument.
 */
public record SparkJobDefinition(
    String mainClass,
    List<String> jarFileUris,
    Map<String, String> properties
) {
}
