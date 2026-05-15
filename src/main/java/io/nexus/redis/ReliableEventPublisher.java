package io.nexus.actor.redis;

import io.nexus.actor.serialize.RpcSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Base64;
import java.util.Map;
import java.util.UUID;

/**
 * 可靠事件发布者。
 * 用于 Master → Slave 的关键变更推送（战斗结果、扣资源等）。
 * 发布到 Redis Stream，由 ReliableEventConsumer 通过 consumer group + XACK 消费。
 */
public class ReliableEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ReliableEventPublisher.class);

    private final StringRedisTemplate redisTemplate;
    private final RpcSerializer serializer;

    public ReliableEventPublisher(StringRedisTemplate redisTemplate, RpcSerializer serializer) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
    }

    /**
     * 发布关键事件到目标 Pod 的可靠 stream。
     */
    public void publish(int targetServerId, String targetPodId, String eventType, Object event) {
        String streamKey = streamKey(targetServerId, targetPodId);
        String eventId = UUID.randomUUID().toString();
        byte[] payload = serializer.serialize(event);
        String encoded = Base64.getEncoder().encodeToString(payload);

        var record = StreamRecords.string(Map.of(
                "id", eventId,
                "type", eventType,
                "d", encoded
        )).withStreamKey(streamKey);

        redisTemplate.opsForStream().add(record);
    }

    /**
     * 发布关键事件到指定玩家所在 Pod。
     */
    public void publishToPlayer(int serverId, String podId, long playerId, String eventType, Object event) {
        String streamKey = streamKey(serverId, podId);
        String eventId = UUID.randomUUID().toString();
        byte[] payload = serializer.serialize(event);
        String encoded = Base64.getEncoder().encodeToString(payload);

        var record = StreamRecords.string(Map.of(
                "id", eventId,
                "type", eventType,
                "pid", String.valueOf(playerId),
                "d", encoded
        )).withStreamKey(streamKey);

        redisTemplate.opsForStream().add(record);
    }

    public static String streamKey(int serverId, String podId) {
        return "event:pod:" + serverId + ":" + podId;
    }
}
