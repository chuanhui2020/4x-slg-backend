package io.nexus.metrics;

import io.nexus.actor.role.MasterLeaseManager;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class MasterLeaseHealthIndicator implements HealthIndicator {

    private final MasterLeaseManager leaseManager;

    public MasterLeaseHealthIndicator(MasterLeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    @Override
    public Health health() {
        Health.Builder builder = leaseManager.isActive() ? Health.up() : Health.outOfService();
        return builder
                .withDetail("role", leaseManager.isActive() ? "ACTIVE" : "STANDBY")
                .withDetail("epoch", leaseManager.getEpoch())
                .build();
    }
}
