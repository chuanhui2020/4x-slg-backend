package io.nexus.actor.redis;

import io.nexus.actor.persistence.StreamPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Base64;

/**
 * AOI 广播层发布者。
 * 使用 Redis Pub/Sub 发布场景变化，延迟最低，fire-and-forget。
 */
public class RedisPubSubPublisher implements StreamPublisher {

    private final StringRedisTemplate redisTemplate;

    public RedisPubSubPublisher(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void publish(String channel, byte[] payload) {
        String encoded = Base64.getEncoder().encodeToString(payload);
        redisTemplate.convertAndSend(channel, encoded);
    }
}
