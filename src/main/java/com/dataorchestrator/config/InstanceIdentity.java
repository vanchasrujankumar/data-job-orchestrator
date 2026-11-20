package com.dataorchestrator.config;

import java.util.UUID;

import org.springframework.stereotype.Component;

/**
 * Identity of this running instance, used as the lease owner when reserving concurrency
 * slots so reconciliation can detect abandoned reservations from crashed replicas.
 */
@Component
public class InstanceIdentity {

    private final String id = UUID.randomUUID().toString();

    /** Returns this instance's unique identifier. */
    public String id() {
        return id;
    }
}
