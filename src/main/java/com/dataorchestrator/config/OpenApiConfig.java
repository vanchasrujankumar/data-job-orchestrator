package com.dataorchestrator.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;

/**
 * OpenAPI/Swagger metadata for the operational REST API. The interactive Swagger UI is
 * served at {@code /swagger-ui.html} and the JSON spec at {@code /v3/api-docs}.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI orchestratorOpenApi() {
        return new OpenAPI()
            .info(new Info()
                .title("Data Job Orchestrator API")
                .description("Operational REST API for managing and monitoring "
                    + "Kafka-driven Google Cloud Dataproc job orchestration.")
                .version("1.0.0"));
    }
}
