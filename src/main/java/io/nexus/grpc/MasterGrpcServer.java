package io.nexus.actor.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MasterGrpcServer {

    private static final Logger log = LoggerFactory.getLogger(MasterGrpcServer.class);

    private final int port;
    private final Server server;

    public MasterGrpcServer(int port, CommandDispatcher dispatcher) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new MasterCommandServiceImpl(dispatcher))
                .build();
    }

    public void start() throws IOException {
        server.start();
        log.info("gRPC server started on port {}", port);
    }

    public void stop() {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                server.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("gRPC server stopped");
        }
    }

    public int getPort() {
        return port;
    }
}
