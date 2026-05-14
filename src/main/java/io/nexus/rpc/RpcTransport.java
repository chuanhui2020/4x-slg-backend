package io.nexus.actor.rpc;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface RpcTransport {

    void sendTell(int targetServerId, ActorEnvelope envelope);

    <T> CompletableFuture<T> sendAsk(int targetServerId, ActorEnvelope envelope, Duration timeout);
}
