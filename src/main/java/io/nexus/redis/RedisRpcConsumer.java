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
import java.util.concurrent.TimeUnit;

/**
 * Master Pod 的 RPC Stream 消费者（保留用于向后兼容）。
 * gRPC 引入后，新的 Slave 走 gRPC；旧 Slave 或跨服通信仍可能走 Stream。
 * 消费 "rpc:master:{serverId}" 上的 tell 消息。
 */
public class RedisRpcConsumer {

    private static final Logger log = LoggerFactory.getLogger(RedisRpcConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final RpcSerializer serializer;
    private final RpcMessageHandler messageHandler;
    private final String streamKey;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "redis-rpc-consumer"));
    private volatile boolean running = false;
    private String lastId = "0";

    public RedisRpcConsumer(StringRedisTemplate redisTemplate, RpcSerializer serializer,
                            RpcMessageHandler messageHandler, int serverId) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.messageHandler = messageHandler;
        this.streamKey = "rpc:master:" + serverId;
    }

    public void start() {
        running = true;
        executor.submit(this::consumeLoop);
        log.info("RPC consumer started on stream: {}", streamKey);
    }

    public void stop() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
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
                            messageHandler.handleIncoming(bytes);
                        }
                    }
                }
            } catch (Exception e) {
                if (running) {
                    log.warn("RPC consume error: {}", e.getMessage());
                    try { Thread.sleep(100); } catch (InterruptedException ignored) { break; }
                }
            }
        }
    }
}
