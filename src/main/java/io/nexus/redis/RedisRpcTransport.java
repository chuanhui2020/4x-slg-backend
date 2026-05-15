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
    private final int serverId;
    private final String podId;
    private final Map<String, CompletableFuture<Object>> pendingAsks = new ConcurrentHashMap<>();

    public RedisRpcTransport(StringRedisTemplate redisTemplate, RpcSerializer serializer,
                             int serverId, String podId) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.serverId = serverId;
        this.podId = podId;
    }

    @Override
    public void sendTell(int targetServerId, ActorEnvelope envelope) {
        String streamKey = "rpc:master:" + targetServerId;
        publish(streamKey, serializer.serialize(envelope));
    }

    @Override
    public void sendTellToPod(int targetServerId, String targetPodId, ActorEnvelope envelope) {
        String streamKey = "rpc:pod:" + targetServerId + ":" + targetPodId;
        publishWithType(streamKey, "tell", serializer.serialize(envelope));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> sendAsk(int targetServerId, ActorEnvelope envelope, Duration timeout) {
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingAsks.put(requestId, future);

        AskEnvelope askEnvelope = new AskEnvelope(requestId, serverId, podId, envelope);
        String streamKey = "rpc:master:" + targetServerId;
        publish(streamKey, serializer.serialize(askEnvelope));

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

    public int getServerId() {
        return serverId;
    }

    public String getResponseStreamKey() {
        return "rpc:pod:" + serverId + ":" + podId;
    }

    private void publish(String streamKey, byte[] payload) {
        String encoded = Base64.getEncoder().encodeToString(payload);
        var record = StreamRecords.string(Map.of("d", encoded)).withStreamKey(streamKey);
        redisTemplate.opsForStream().add(record);
    }

    private void publishWithType(String streamKey, String type, byte[] payload) {
        String encoded = Base64.getEncoder().encodeToString(payload);
        var record = StreamRecords.string(Map.of("t", type, "d", encoded)).withStreamKey(streamKey);
        redisTemplate.opsForStream().add(record);
    }

    public void sendResponseToPod(int targetServerId, String targetPodId, String requestId, Object result) {
        String streamKey = "rpc:pod:" + targetServerId + ":" + targetPodId;
        RedisRpcResponseConsumer.AskResponse response = new RedisRpcResponseConsumer.AskResponse(requestId, result);
        publishWithType(streamKey, "resp", serializer.serialize(response));
    }

    public record AskEnvelope(String requestId, int replyServerId, String replyPodId, ActorEnvelope envelope) {}
}
