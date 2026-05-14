package io.nexus.actor.lifecycle;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class GracefulShutdown {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdown.class);

    private final ActorSystem<?> system;
    private final List<FlushEntry<?>> flushEntries = new CopyOnWriteArrayList<>();

    public GracefulShutdown(ActorSystem<?> system) {
        this.system = system;
    }

    public <T> void register(ActorRef<T> actor, Function<ActorRef<FlushableActor.FlushComplete>, T> flushMessageFactory) {
        flushEntries.add(new FlushEntry<>(actor, flushMessageFactory));
    }

    public void unregister(ActorRef<?> actor) {
        flushEntries.removeIf(entry -> entry.actor().equals(actor));
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> shutdown(Duration timeout) {
        log.info("Graceful shutdown initiated, flushing {} actors", flushEntries.size());

        if (flushEntries.isEmpty()) {
            return terminateSystem();
        }

        List<CompletableFuture<FlushableActor.FlushComplete>> futures = flushEntries.stream()
                .map(entry -> askFlush((FlushEntry<Object>) entry, timeout))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .handle((v, ex) -> {
                    if (ex != null) {
                        log.warn("Flush timeout or error, forcing termination: {}", ex.getMessage());
                    } else {
                        log.info("All actors flushed successfully");
                    }
                    return null;
                })
                .thenCompose(v -> terminateSystem());
    }

    private <T> CompletableFuture<FlushableActor.FlushComplete> askFlush(FlushEntry<T> entry, Duration timeout) {
        return AskPattern.ask(
                entry.actor(),
                (ActorRef<FlushableActor.FlushComplete> replyTo) -> entry.flushMessageFactory().apply(replyTo),
                timeout,
                system.scheduler()
        ).toCompletableFuture();
    }

    private CompletableFuture<Void> terminateSystem() {
        system.terminate();
        return system.getWhenTerminated().thenApply(done -> (Void) null).toCompletableFuture();
    }

    private record FlushEntry<T>(ActorRef<T> actor, Function<ActorRef<FlushableActor.FlushComplete>, T> flushMessageFactory) {}
}
