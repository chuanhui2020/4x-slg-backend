package io.nexus.actor.redis;

import io.nexus.actor.serialize.RpcSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Pod 级 RPC 响应消费者。
 * 消费 "rpc:pod:{podId}" 上的 ask 响应消息。
 */
public class RedisRpcResponseConsumer {

    private static final Logger log = LoggerFactory.getLogger(RedisRpcResponseConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final RpcSerializer serializer;
    private final RedisRpcTransport rpcTransport;
    private final String streamKey;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean running = false;
    private String lastId = "0";

    public RedisRpcResponseConsumer(StringRedisTemplate redisTemplate, RpcSerializer serializer,
                                     RedisRpcTransport rpcTransport) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.rpcTransport = rpcTransport;
        this.streamKey = rpcTransport.getResponseStreamKey();
    }

    public void start() {
        running = true;
        executor.submit(this::consumeLoop);
        log.info("RPC response consumer started on stream: {}", streamKey);
    }

    public void stop() {
        running = false;
        executor.shutdown();
    }

    @SuppressWarnings("unchecked")
    private void consumeLoop() {
        while (running) {
            try {
                List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                        StreamReadOptions.empty().count(100).block(Duration.ofMillis(50)),
                        StreamOffset.create(streamKey, ReadOffset.from(lastId))
                );
                if (records != null) {
                    for (MapRecord<String, Object, Object> record : records) {
                        lastId = record.getId().getValue();
                        Object payload = record.getValue().get("d");
                        if (payload instanceof String encoded) {
                            byte[] bytes = Base64.getDecoder().decode(encoded);
                            processResponse(bytes);
                        }
                    }
                }
            } catch (Exception e) {
                if (running) {
                    log.warn("RPC response consume error: {}", e.getMessage());
                    try { Thread.sleep(100); } catch (InterruptedException ignored) { break; }
                }
            }
        }
    }

    private void processResponse(byte[] data) {
        try {
            AskResponse response = serializer.deserialize(data, AskResponse.class);
            if (response != null) {
                rpcTransport.handleAskResponse(response.requestId(), response.result());
            }
        } catch (Exception e) {
            log.error("Failed to process RPC response", e);
        }
    }

    public record AskResponse(String requestId, Object result) {}
}
