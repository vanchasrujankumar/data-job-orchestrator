# Build the jar first:  ./gradlew bootJar
# Then build the image: docker compose build app
FROM eclipse-temurin:21-jre

WORKDIR /app
COPY build/libs/data-job-orchestrator-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8080

# Run as a non-root user for security.
RUN useradd --create-home --uid 10001 appuser
USER appuser

ENTRYPOINT ["java", "-jar", "app.jar"]
