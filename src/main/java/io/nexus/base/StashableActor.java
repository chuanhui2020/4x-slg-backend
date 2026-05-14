package io.nexus.actor.base;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.util.concurrent.CompletableFuture;

public abstract class StashableActor<T> extends AbstractBehavior<T> {

    private final StashBuffer<T> stash;

    protected StashableActor(ActorContext<T> context, StashBuffer<T> stash) {
        super(context);
        this.stash = stash;
    }

    protected abstract CompletableFuture<Void> loadAsync();

    protected abstract Receive<T> active();

    protected abstract T buildLoadCompleteMessage();

    protected Receive<T> loading() {
        return newReceiveBuilder()
                .onMessageEquals(buildLoadCompleteMessage(), () -> stash.unstashAll(active()))
                .onAnyMessage(msg -> {
                    stash.stash(msg);
                    return Behaviors.same();
                })
                .build();
    }

    protected void startAsyncLoad() {
        var self = getContext().getSelf();
        var loadCompleteMsg = buildLoadCompleteMessage();
        loadAsync().whenComplete((v, ex) -> {
            if (ex != null) {
                getContext().getLog().error("Async load failed", ex);
            }
            self.tell(loadCompleteMsg);
        });
    }
}
