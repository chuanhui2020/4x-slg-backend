package io.nexus.actor.redis;

import io.nexus.actor.persistence.StreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RedisStreamConsumer implements StreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(RedisStreamConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final Map<String, StreamSubscription> subscriptions = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public RedisStreamConsumer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void subscribe(String streamKey, Consumer<byte[]> handler) {
        if (subscriptions.containsKey(streamKey)) return;

        StreamSubscription sub = new StreamSubscription(streamKey, handler);
        subscriptions.put(streamKey, sub);
        executor.submit(sub);
    }

    @Override
    public void unsubscribe(String streamKey) {
        StreamSubscription sub = subscriptions.remove(streamKey);
        if (sub != null) {
            sub.stop();
        }
    }

    public void shutdown() {
        subscriptions.values().forEach(StreamSubscription::stop);
        subscriptions.clear();
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

    private class StreamSubscription implements Runnable {
        private final String streamKey;
        private final Consumer<byte[]> handler;
        private volatile boolean running = true;
        private String lastId = "$";

        StreamSubscription(String streamKey, Consumer<byte[]> handler) {
            this.streamKey = streamKey;
            this.handler = handler;
        }

        void stop() {
            running = false;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            while (running) {
                try {
                    List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                            StreamReadOptions.empty().count(100).block(Duration.ofMillis(100)),
                            StreamOffset.create(streamKey, ReadOffset.from(lastId))
                    );
                    if (records != null) {
                        for (MapRecord<String, Object, Object> record : records) {
                            lastId = record.getId().getValue();
                            Object payload = record.getValue().get("d");
                            if (payload instanceof String encoded) {
                                byte[] bytes = Base64.getDecoder().decode(encoded);
                                handler.accept(bytes);
                            }
                        }
                    }
                } catch (Exception e) {
                    if (running) {
                        log.warn("Stream read error on {}: {}", streamKey, e.getMessage());
                        try { Thread.sleep(500); } catch (InterruptedException ignored) { break; }
                    }
                }
            }
        }
    }
}
