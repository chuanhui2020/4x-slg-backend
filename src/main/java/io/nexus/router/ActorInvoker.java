package io.nexus.actor.router;

import io.nexus.actor.address.ActorAddress;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface ActorInvoker {

    void tell(ActorAddress target, Object message);

    <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout);
}
