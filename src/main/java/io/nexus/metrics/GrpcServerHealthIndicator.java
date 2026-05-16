package io.nexus.metrics;

import io.nexus.actor.grpc.MasterGrpcServer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class GrpcServerHealthIndicator implements HealthIndicator {

    private final MasterGrpcServer grpcServer;

    public GrpcServerHealthIndicator(MasterGrpcServer grpcServer) {
        this.grpcServer = grpcServer;
    }

    @Override
    public Health health() {
        if (grpcServer == null) {
            return Health.unknown().withDetail("reason", "gRPC server not initialized (slave pod)").build();
        }
        return Health.up()
                .withDetail("port", grpcServer.getPort())
                .build();
    }
}
