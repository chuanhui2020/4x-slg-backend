package io.nexus.actor.rpc;

import io.nexus.actor.address.ActorAddress;

public record ActorEnvelope(ActorAddress target, Object message, ActorAddress replyTo) {

    public ActorEnvelope(ActorAddress target, Object message) {
        this(target, message, null);
    }
}
