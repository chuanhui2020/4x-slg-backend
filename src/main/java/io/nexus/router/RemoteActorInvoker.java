package io.nexus.actor.router;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.address.ActorType;
import io.nexus.actor.grpc.MasterGrpcClient;
import io.nexus.actor.rpc.ActorEnvelope;
import io.nexus.actor.rpc.RpcTransport;
import io.nexus.actor.serialize.RpcSerializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class RemoteActorInvoker implements ActorInvoker {

    private final RpcTransport transport;
    private final MasterGrpcClient grpcClient;
    private final RpcSerializer serializer;

    public RemoteActorInvoker(RpcTransport transport) {
        this(transport, null, null);
    }

    public RemoteActorInvoker(RpcTransport transport, MasterGrpcClient grpcClient, RpcSerializer serializer) {
        this.transport = transport;
        this.grpcClient = grpcClient;
        this.serializer = serializer;
    }

    @Override
    public void tell(ActorAddress target, Object message) {
        ActorEnvelope envelope = new ActorEnvelope(target, message);
        if (target.type() == ActorType.PLAYER && target.podId() != null) {
            transport.sendTellToPod(target.serverId(), target.podId(), envelope);
        } else {
            transport.sendTell(target.serverId(), envelope);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout) {
        if (grpcClient != null && isMasterActor(target.type())) {
            byte[] payload = serializer.serialize(message);
            String commandType = resolveCommandType(message);
            return (CompletableFuture<T>) grpcClient.execute(commandType, 0, payload, timeout.toMillis())
                    .thenApply(resp -> {
                        if (resp.getSuccess()) {
                            return serializer.deserialize(resp.getPayload().toByteArray(), Object.class);
                        }
                        throw new RuntimeException(resp.getErrorCode() + ": " + resp.getErrorMessage());
                    });
        }
        ActorEnvelope envelope = new ActorEnvelope(target, message);
        return transport.sendAsk(target.serverId(), envelope, timeout);
    }

    private boolean isMasterActor(ActorType type) {
        return switch (type) {
            case AREA, MARCH_SUPERVISOR, RALLY_SUPERVISOR, GUILD, EVENT, MAP_SCENE, WORLD_BOSS, GUILD_WAR -> true;
            case PLAYER -> false;
        };
    }

    private String resolveCommandType(Object message) {
        return message.getClass().getSimpleName().toUpperCase();
    }
}
