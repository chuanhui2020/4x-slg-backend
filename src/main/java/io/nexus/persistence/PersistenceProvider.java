package io.nexus.actor.persistence;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface PersistenceProvider<K, V> {

    CompletableFuture<V> load(K key);

    CompletableFuture<Void> save(K key, V value);

    CompletableFuture<Void> delete(K key);

    CompletableFuture<Void> batchSave(Map<K, V> entries);
}
