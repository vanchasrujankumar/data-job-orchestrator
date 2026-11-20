package com.dataorchestrator.config;

import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * Configures OpenTelemetry (metrics + tracing) as the observability backend, intentionally
 * replacing Micrometer.
 *
 * <p>When {@code orchestrator.otel.enabled=false} a no-op SDK is used (suited to tests).
 * When an OTLP endpoint is configured, telemetry is exported over OTLP/HTTP; otherwise a
 * console/logging exporter is used for local development.</p>
 */
@Configuration
public class OpenTelemetryConfig {

    public static final String INSTRUMENTATION_SCOPE = "data-job-orchestrator";

    @Bean
    OpenTelemetry openTelemetry(OrchestratorProperties props) {
        OrchestratorProperties.Otel otel = props.otel();
        if (!otel.enabled()) {
            return OpenTelemetry.noop();
        }

        Resource resource = Resource.builder()
            .put(AttributeKey.stringKey("service.name"), otel.serviceName())
            .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .setResource(resource)
            .addSpanProcessor(BatchSpanProcessor.builder(spanExporter(otel)).build())
            .build();

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(PeriodicMetricReader.builder(metricExporter(otel))
                .setInterval(Duration.ofSeconds(30))
                .build())
            .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .build();

        // Best-effort flush of in-memory spans/metrics on shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            tracerProvider.close();
            meterProvider.close();
        }));
        return sdk;
    }

    @Bean
    Meter meter(OpenTelemetry openTelemetry) {
        return openTelemetry.getMeter(INSTRUMENTATION_SCOPE);
    }

    @Bean
    Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    }

    private SpanExporter spanExporter(OrchestratorProperties.Otel otel) {
        if (otel.otlpEndpoint() != null && !otel.otlpEndpoint().isBlank()) {
            return OtlpHttpSpanExporter.builder()
                .setEndpoint(otel.otlpEndpoint() + "/v1/traces")
                .build();
        }
        return LoggingSpanExporter.create();
    }

    private MetricExporter metricExporter(OrchestratorProperties.Otel otel) {
        if (otel.otlpEndpoint() != null && !otel.otlpEndpoint().isBlank()) {
            return OtlpHttpMetricExporter.builder()
                .setEndpoint(otel.otlpEndpoint() + "/v1/metrics")
                .build();
        }
        return LoggingMetricExporter.create();
    }
}
