package io.nexus.actor.redis;

import io.nexus.actor.persistence.StreamPublisher;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Base64;
import java.util.Map;

public class RedisStreamPublisher implements StreamPublisher {

    private final StringRedisTemplate redisTemplate;

    public RedisStreamPublisher(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void publish(String streamKey, byte[] payload) {
        String encoded = Base64.getEncoder().encodeToString(payload);
        var record = StreamRecords.string(Map.of("d", encoded)).withStreamKey(streamKey);
        redisTemplate.opsForStream().add(record);
    }
}
