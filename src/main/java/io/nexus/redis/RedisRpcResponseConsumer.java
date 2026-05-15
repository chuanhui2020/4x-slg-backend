package io.nexus.actor.redis;

/**
 * Ask 响应消息格式。
 * 保留用于向后兼容（跨服通信场景）。
 */
public class RedisRpcResponseConsumer {

    private RedisRpcResponseConsumer() {}

    public record AskResponse(String requestId, Object result) {}
}
