package io.nexus.actor.router;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.registry.ActorRegistry;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class LocalActorInvoker implements ActorInvoker {

    private final ActorRegistry registry;
    private final ActorSystem<?> system;

    public LocalActorInvoker(ActorRegistry registry, ActorSystem<?> system) {
        this.registry = registry;
        this.system = system;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void tell(ActorAddress target, Object message) {
        ActorRef<Object> ref = registry.resolveOrThrow(target.type(), target.id());
        ref.tell(message);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout) {
        ActorRef<Object> ref = registry.resolveOrThrow(target.type(), target.id());
        return AskPattern.<Object, T>ask(
                ref,
                replyTo -> new AskWrapper<>(message, (ActorRef<Object>) replyTo),
                timeout,
                system.scheduler()
        ).toCompletableFuture();
    }

    public record AskWrapper<T>(Object message, ActorRef<T> replyTo) {}
}
