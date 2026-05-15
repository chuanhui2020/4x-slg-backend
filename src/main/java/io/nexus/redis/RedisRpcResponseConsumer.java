package io.nexus.actor.redis;

/**
 * Ask 响应消息格式。
 * 由 RedisPodConsumer 统一消费。
 */
public class RedisRpcResponseConsumer {

    private RedisRpcResponseConsumer() {}

    public record AskResponse(String requestId, Object result) {}
}
