package io.nexus.actor.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.nexus.actor.grpc.proto.CommandRequest;
import io.nexus.actor.grpc.proto.CommandResponse;
import io.nexus.actor.grpc.proto.MasterCommandServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class MasterGrpcClient {

    private static final Logger log = LoggerFactory.getLogger(MasterGrpcClient.class);
    private static final long DEFAULT_DEADLINE_MS = 2000;

    private final ManagedChannel channel;
    private final MasterCommandServiceGrpc.MasterCommandServiceFutureStub futureStub;
    private final int serverId;
    private final String podId;

    public MasterGrpcClient(String target, int serverId, String podId) {
        this.serverId = serverId;
        this.podId = podId;
        this.channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        this.futureStub = MasterCommandServiceGrpc.newFutureStub(channel);
        log.info("gRPC client connected to {}", target);
    }

    public CompletableFuture<CommandResponse> execute(String commandType, long playerId, byte[] payload) {
        return execute(commandType, playerId, payload, DEFAULT_DEADLINE_MS);
    }

    public CompletableFuture<CommandResponse> execute(String commandType, long playerId,
                                                      byte[] payload, long deadlineMs) {
        CommandRequest request = CommandRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setPlayerId(playerId)
                .setServerId(serverId)
                .setSourcePodId(podId)
                .setCommandType(commandType)
                .setPayload(ByteString.copyFrom(payload))
                .setDeadlineMs(deadlineMs)
                .build();

        var grpcFuture = futureStub
                .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                .execute(request);

        CompletableFuture<CommandResponse> result = new CompletableFuture<>();
        grpcFuture.addListener(() -> {
            try {
                result.complete(grpcFuture.get());
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        }, Runnable::run);

        return result;
    }

    public void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
