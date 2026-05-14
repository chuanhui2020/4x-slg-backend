package io.nexus.actor.persistence;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface CacheProvider {

    CompletableFuture<byte[]> get(String key);

    CompletableFuture<Void> put(String key, byte[] value);

    CompletableFuture<Void> putAll(Map<String, byte[]> entries);

    CompletableFuture<Void> remove(String... keys);
}
