package com.dataorchestrator.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.EnableMongoAuditing;

/**
 * MongoDB configuration: enables auditing ({@code @CreatedDate}/{@code @LastModifiedDate})
 * and registers the transaction manager used by {@code @Transactional} service methods so
 * the transactional outbox is atomic (requires a replica set — see docker-compose.yml).
 */
@Configuration
@EnableMongoAuditing
public class MongoConfig {

    @Bean
    public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory databaseFactory) {
        return new MongoTransactionManager(databaseFactory);
    }
}
