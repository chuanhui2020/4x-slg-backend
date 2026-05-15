package io.nexus.actor.rpc;

import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.serialize.RpcSerializer;
import org.apache.pekko.actor.typed.ActorRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(RpcMessageHandler.class);

    private final ActorRegistry registry;
    private final RpcSerializer serializer;

    public RpcMessageHandler(ActorRegistry registry, RpcSerializer serializer) {
        this.registry = registry;
        this.serializer = serializer;
    }

    public void handleIncoming(byte[] data) {
        ActorEnvelope envelope = serializer.deserialize(data, ActorEnvelope.class);
        dispatch(envelope);
    }

    @SuppressWarnings("unchecked")
    public void dispatch(ActorEnvelope envelope) {
        var target = envelope.target();
        var refOpt = registry.resolve(target.type(), target.id());
        if (refOpt.isPresent()) {
            ActorRef<Object> ref = (ActorRef<Object>) refOpt.get();
            ref.tell(envelope.message());
        } else {
            log.warn("Actor not found for RPC dispatch: {}", target);
        }
    }

    @SuppressWarnings("unchecked")
    public Object dispatchAndReturn(ActorEnvelope envelope) {
        var target = envelope.target();
        var refOpt = registry.resolve(target.type(), target.id());
        if (refOpt.isPresent()) {
            ActorRef<Object> ref = (ActorRef<Object>) refOpt.get();
            ref.tell(envelope.message());
            return null;
        } else {
            log.warn("Actor not found for RPC ask dispatch: {}", target);
            return null;
        }
    }
}
