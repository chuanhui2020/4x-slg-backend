package io.nexus.actor.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 玩家位置表。
 * Slave 登录时写入，定期续约，断线时删除。
 * Master 通过此表定位玩家所在 Pod。
 *
 * Key: player_location:{serverId}:{playerId} = podId
 * TTL: 120s，续约间隔 40s
 */
public class PlayerLocationRegistry {

    private static final Logger log = LoggerFactory.getLogger(PlayerLocationRegistry.class);
    private static final Duration TTL = Duration.ofSeconds(120);
    private static final long RENEW_INTERVAL_SECONDS = 40;

    private final StringRedisTemplate redisTemplate;
    private final int serverId;
    private final String podId;
    private final Map<Long, Boolean> localPlayers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "player-location-registry"));

    public PlayerLocationRegistry(StringRedisTemplate redisTemplate, int serverId, String podId) {
        this.redisTemplate = redisTemplate;
        this.serverId = serverId;
        this.podId = podId;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::renewAll, RENEW_INTERVAL_SECONDS, RENEW_INTERVAL_SECONDS, TimeUnit.SECONDS);
        log.info("Player location registry started, serverId={}, podId={}", serverId, podId);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        localPlayers.keySet().forEach(this::unregister);
        localPlayers.clear();
    }

    public void register(long playerId) {
        localPlayers.put(playerId, Boolean.TRUE);
        String key = locationKey(playerId);
        redisTemplate.opsForValue().set(key, podId, TTL);
    }

    public void unregister(long playerId) {
        localPlayers.remove(playerId);
        String key = locationKey(playerId);
        String current = redisTemplate.opsForValue().get(key);
        if (podId.equals(current)) {
            redisTemplate.delete(key);
        }
    }

    public String lookup(int serverId, long playerId) {
        String key = "player_location:" + serverId + ":" + playerId;
        return redisTemplate.opsForValue().get(key);
    }

    private void renewAll() {
        for (long playerId : localPlayers.keySet()) {
            try {
                String key = locationKey(playerId);
                redisTemplate.expire(key, TTL);
            } catch (Exception e) {
                log.warn("Failed to renew location for player {}: {}", playerId, e.getMessage());
            }
        }
    }

    private String locationKey(long playerId) {
        return "player_location:" + serverId + ":" + playerId;
    }
}
