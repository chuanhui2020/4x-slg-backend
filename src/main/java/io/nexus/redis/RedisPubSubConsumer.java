package io.nexus.actor.redis;

import io.nexus.actor.persistence.StreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * AOI 广播层消费者。
 * 使用 Redis Pub/Sub 订阅场景变化 channel，延迟最低。
 * 丢消息可接受（下一帧会补）。
 */
public class RedisPubSubConsumer implements StreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(RedisPubSubConsumer.class);

    private final RedisMessageListenerContainer listenerContainer;
    private final Map<String, PubSubSubscription> subscriptions = new ConcurrentHashMap<>();

    public RedisPubSubConsumer(RedisMessageListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
    }

    @Override
    public void subscribe(String channel, Consumer<byte[]> handler) {
        if (subscriptions.containsKey(channel)) return;

        PubSubSubscription subscription = new PubSubSubscription(handler);
        ChannelTopic topic = new ChannelTopic(channel);
        listenerContainer.addMessageListener(subscription, topic);
        subscriptions.put(channel, subscription);
    }

    @Override
    public void unsubscribe(String channel) {
        PubSubSubscription subscription = subscriptions.remove(channel);
        if (subscription != null) {
            listenerContainer.removeMessageListener(subscription);
        }
    }

    public void shutdown() {
        subscriptions.forEach((channel, sub) -> listenerContainer.removeMessageListener(sub));
        subscriptions.clear();
    }

    private static class PubSubSubscription implements MessageListener {

        private final Consumer<byte[]> handler;

        PubSubSubscription(Consumer<byte[]> handler) {
            this.handler = handler;
        }

        @Override
        public void onMessage(Message message, byte[] pattern) {
            try {
                String encoded = new String(message.getBody());
                byte[] decoded = Base64.getDecoder().decode(encoded);
                handler.accept(decoded);
            } catch (Exception e) {
                log.warn("Failed to process pub/sub message: {}", e.getMessage());
            }
        }
    }
}
