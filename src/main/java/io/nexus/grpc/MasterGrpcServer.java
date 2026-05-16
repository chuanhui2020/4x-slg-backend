package io.nexus.actor.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.nexus.metrics.GameMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MasterGrpcServer {

    private static final Logger log = LoggerFactory.getLogger(MasterGrpcServer.class);

    private final int port;
    private final Server server;

    public MasterGrpcServer(int port, CommandDispatcher dispatcher, GameMetrics metrics) {
        this.port = port;
        this.server = NettyServerBuilder.forPort(port)
                .addService(new MasterCommandServiceImpl(dispatcher, metrics))
                .maxConcurrentCallsPerConnection(256)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .permitKeepAliveTime(10, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true)
                .maxInboundMessageSize(16 * 1024 * 1024)
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
