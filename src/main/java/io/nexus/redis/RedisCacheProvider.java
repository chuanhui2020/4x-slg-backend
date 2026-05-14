package io.nexus.actor.redis;

import io.nexus.actor.persistence.CacheProvider;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RedisCacheProvider implements CacheProvider {

    private final StringRedisTemplate redisTemplate;

    public RedisCacheProvider(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public CompletableFuture<byte[]> get(String key) {
        return CompletableFuture.supplyAsync(() -> {
            String value = redisTemplate.opsForValue().get(key);
            return value != null ? Base64.getDecoder().decode(value) : null;
        });
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return CompletableFuture.runAsync(() ->
                redisTemplate.opsForValue().set(key, Base64.getEncoder().encodeToString(value)));
    }

    @Override
    public CompletableFuture<Void> putAll(Map<String, byte[]> entries) {
        return CompletableFuture.runAsync(() -> {
            Map<String, String> encoded = new HashMap<>();
            entries.forEach((k, v) -> encoded.put(k, Base64.getEncoder().encodeToString(v)));
            redisTemplate.opsForValue().multiSet(encoded);
        });
    }

    @Override
    public CompletableFuture<Void> remove(String... keys) {
        return CompletableFuture.runAsync(() -> {
            for (String key : keys) {
                redisTemplate.delete(key);
            }
        });
    }
}
