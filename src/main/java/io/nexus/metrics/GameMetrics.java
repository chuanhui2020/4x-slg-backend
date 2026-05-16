package io.nexus.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class GameMetrics implements MeterBinder {

    private static final String PREFIX = "game.";

    private MeterRegistry registry;

    // gRPC
    private Timer grpcRequestTimer;
    private Counter grpcRequestErrorCounter;
    private AtomicInteger grpcConcurrentRequests;

    // CommandDispatcher
    private Timer commandDispatchTimer;
    private Counter commandDispatchErrorCounter;
    private AtomicInteger commandConcurrentDispatches;

    // MasterLease
    private AtomicLong leaseEpoch;
    private Counter leaseRenewalFailureCounter;
    private Timer leaseRenewalTimer;

    @Override
    public void bindTo(MeterRegistry registry) {
        this.registry = registry;

        // gRPC metrics
        grpcRequestTimer = Timer.builder(PREFIX + "grpc.request.duration")
                .description("gRPC request duration")
                .tag("service", "MasterCommandService")
                .register(registry);

        grpcRequestErrorCounter = Counter.builder(PREFIX + "grpc.request.errors")
                .description("gRPC request errors")
                .register(registry);

        grpcConcurrentRequests = registry.gauge(PREFIX + "grpc.request.concurrent",
                new AtomicInteger(0));

        // CommandDispatcher metrics
        commandDispatchTimer = Timer.builder(PREFIX + "command.dispatch.duration")
                .description("Command dispatch duration")
                .register(registry);

        commandDispatchErrorCounter = Counter.builder(PREFIX + "command.dispatch.errors")
                .description("Command dispatch errors")
                .register(registry);

        commandConcurrentDispatches = registry.gauge(PREFIX + "command.dispatch.concurrent",
                new AtomicInteger(0));

        // Lease metrics
        leaseEpoch = registry.gauge(PREFIX + "master.lease.epoch",
                new AtomicLong(0));

        leaseRenewalFailureCounter = Counter.builder(PREFIX + "master.lease.renewal.failures")
                .description("Lease renewal failures")
                .register(registry);

        leaseRenewalTimer = Timer.builder(PREFIX + "master.lease.renewal.duration")
                .description("Lease renewal duration")
                .register(registry);
    }

    public Timer grpcRequestTimer() { return grpcRequestTimer; }
    public Counter grpcRequestErrorCounter() { return grpcRequestErrorCounter; }
    public AtomicInteger grpcConcurrentRequests() { return grpcConcurrentRequests; }

    public Timer commandDispatchTimer() { return commandDispatchTimer; }
    public Counter commandDispatchErrorCounter() { return commandDispatchErrorCounter; }
    public AtomicInteger commandConcurrentDispatches() { return commandConcurrentDispatches; }

    public AtomicLong leaseEpoch() { return leaseEpoch; }
    public Counter leaseRenewalFailureCounter() { return leaseRenewalFailureCounter; }
    public Timer leaseRenewalTimer() { return leaseRenewalTimer; }

    public Timer timer(String name, String... tags) {
        return Timer.builder(PREFIX + name).tags(tags).register(registry);
    }

    public Counter counter(String name, String... tags) {
        return Counter.builder(PREFIX + name).tags(tags).register(registry);
    }
}
