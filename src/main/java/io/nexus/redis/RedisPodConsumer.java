package io.nexus.actor.redis;

import io.nexus.actor.rpc.RpcMessageHandler;
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
 * Pod 级统一消息消费者。
 * 消费 "rpc:pod:{serverId}:{podId}" 上的消息，包括：
 * - 来自其他 Pod 的 tell 消息（ActorEnvelope）
 * - ask 响应消息（AskResponse）
 */
public class RedisPodConsumer {

    private static final Logger log = LoggerFactory.getLogger(RedisPodConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final RpcSerializer serializer;
    private final RpcMessageHandler messageHandler;
    private final RedisRpcTransport rpcTransport;
    private final String streamKey;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean running = false;
    private String lastId = "0";

    public RedisPodConsumer(StringRedisTemplate redisTemplate, RpcSerializer serializer,
                            RpcMessageHandler messageHandler, RedisRpcTransport rpcTransport) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.messageHandler = messageHandler;
        this.rpcTransport = rpcTransport;
        this.streamKey = rpcTransport.getResponseStreamKey();
    }

    public void start() {
        running = true;
        executor.submit(this::consumeLoop);
        log.info("Pod consumer started on stream: {}", streamKey);
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
                        Map<Object, Object> values = record.getValue();
                        Object type = values.get("t");
                        Object payload = values.get("d");
                        if (payload instanceof String encoded) {
                            byte[] bytes = Base64.getDecoder().decode(encoded);
                            if ("resp".equals(type)) {
                                processResponse(bytes);
                            } else {
                                processTell(bytes);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (running) {
                    log.warn("Pod consume error: {}", e.getMessage());
                    try { Thread.sleep(100); } catch (InterruptedException ignored) { break; }
                }
            }
        }
    }

    private void processTell(byte[] data) {
        try {
            messageHandler.handleIncoming(data);
        } catch (Exception e) {
            log.error("Failed to process pod tell message", e);
        }
    }

    private void processResponse(byte[] data) {
        try {
            RedisRpcResponseConsumer.AskResponse response = serializer.deserialize(data, RedisRpcResponseConsumer.AskResponse.class);
            if (response != null) {
                rpcTransport.handleAskResponse(response.requestId(), response.result());
            }
        } catch (Exception e) {
            log.error("Failed to process pod ask response", e);
        }
    }
}
