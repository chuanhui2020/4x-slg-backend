package io.nexus.actor.persistence;

import java.util.function.Consumer;

public interface StreamConsumer {

    void subscribe(String streamKey, Consumer<byte[]> handler);

    void unsubscribe(String streamKey);
}
