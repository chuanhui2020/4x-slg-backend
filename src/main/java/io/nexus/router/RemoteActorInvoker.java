package io.nexus.actor.router;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.address.ActorType;
import io.nexus.actor.rpc.ActorEnvelope;
import io.nexus.actor.rpc.RpcTransport;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class RemoteActorInvoker implements ActorInvoker {

    private final RpcTransport transport;

    public RemoteActorInvoker(RpcTransport transport) {
        this.transport = transport;
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
    public <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout) {
        ActorEnvelope envelope = new ActorEnvelope(target, message);
        return transport.sendAsk(target.serverId(), envelope, timeout);
    }
}
