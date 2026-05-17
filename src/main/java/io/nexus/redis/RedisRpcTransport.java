package io.nexus.actor.redis;

import io.nexus.actor.rpc.ActorEnvelope;
import io.nexus.actor.rpc.RpcTransport;
import io.nexus.actor.serialize.RpcSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class RedisRpcTransport implements RpcTransport {

    private static final Logger log = LoggerFactory.getLogger(RedisRpcTransport.class);

    private final StringRedisTemplate redisTemplate;
    private final RpcSerializer serializer;
    private final int serverId;
    private final String podId;
    private final int maxPendingAsks;
    private final Map<String, CompletableFuture<Object>> pendingAsks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupScheduler;

    public RedisRpcTransport(StringRedisTemplate redisTemplate, RpcSerializer serializer,
                             int serverId, String podId, int maxPendingAsks, long cleanupIntervalSeconds) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.serverId = serverId;
        this.podId = podId;
        this.maxPendingAsks = maxPendingAsks;
        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "rpc-transport-cleanup-" + serverId));
        this.cleanupScheduler.scheduleAtFixedRate(this::cleanupExpired,
                cleanupIntervalSeconds, cleanupIntervalSeconds, TimeUnit.SECONDS);
    }

    public void shutdown() {
        cleanupScheduler.shutdown();
        try {
            cleanupScheduler.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pendingAsks.values().forEach(f -> f.completeExceptionally(new CancellationException("Transport shutdown")));
        pendingAsks.clear();
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
        if (pendingAsks.size() >= maxPendingAsks) {
            log.warn("Pending asks limit reached ({}), rejecting request", maxPendingAsks);
            return CompletableFuture.failedFuture(
                    new RejectedExecutionException("Too many pending asks: " + maxPendingAsks));
        }

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

    private void cleanupExpired() {
        int removed = 0;
        for (var entry : pendingAsks.entrySet()) {
            if (entry.getValue().isDone()) {
                pendingAsks.remove(entry.getKey());
                removed++;
            }
        }
        if (removed > 0) {
            log.debug("Cleaned up {} completed pending asks, remaining: {}", removed, pendingAsks.size());
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
