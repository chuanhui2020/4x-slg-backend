package io.nexus.actor.redis;

import io.nexus.actor.rpc.ActorEnvelope;
import io.nexus.actor.rpc.RpcTransport;
import io.nexus.actor.serialize.RpcSerializer;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RedisRpcTransport implements RpcTransport {

    private final StringRedisTemplate redisTemplate;
    private final RpcSerializer serializer;
    private final String podId;
    private final Map<String, CompletableFuture<Object>> pendingAsks = new ConcurrentHashMap<>();

    public RedisRpcTransport(StringRedisTemplate redisTemplate, RpcSerializer serializer) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.podId = UUID.randomUUID().toString().substring(0, 8);
    }

    public RedisRpcTransport(StringRedisTemplate redisTemplate, RpcSerializer serializer, String podId) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.podId = podId;
    }

    @Override
    public void sendTell(int targetServerId, ActorEnvelope envelope) {
        String streamKey = "rpc:master:" + targetServerId;
        byte[] payload = serializer.serialize(envelope);
        String encoded = Base64.getEncoder().encodeToString(payload);
        var record = StreamRecords.string(Map.of("d", encoded)).withStreamKey(streamKey);
        redisTemplate.opsForStream().add(record);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> sendAsk(int targetServerId, ActorEnvelope envelope, Duration timeout) {
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingAsks.put(requestId, future);

        AskEnvelope askEnvelope = new AskEnvelope(requestId, podId, envelope);
        String streamKey = "rpc:master:" + targetServerId;
        byte[] payload = serializer.serialize(askEnvelope);
        String encoded = Base64.getEncoder().encodeToString(payload);
        var record = StreamRecords.string(Map.of("d", encoded)).withStreamKey(streamKey);
        redisTemplate.opsForStream().add(record);

        future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((v, ex) -> pendingAsks.remove(requestId));

        return (CompletableFuture<T>) future;
    }

    public void handleAskResponse(String requestId, Object result) {
        CompletableFuture<Object> future = pendingAsks.remove(requestId);
        if (future != null) {
            future.complete(result);
        }
    }

    public String getPodId() {
        return podId;
    }

    public String getResponseStreamKey() {
        return "rpc:pod:" + podId;
    }

    public record AskEnvelope(String requestId, String replyPodId, ActorEnvelope envelope) {}
}
