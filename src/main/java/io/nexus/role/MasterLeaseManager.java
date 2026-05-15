package io.nexus.actor.role;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Master 租约管理。
 * Active Master 持续续约；Standby 检测 lease 过期后抢主。
 * 每次换主 epoch 递增，关键写入带 epoch 防止旧主复活后继续写。
 */
public class MasterLeaseManager {

    private static final Logger log = LoggerFactory.getLogger(MasterLeaseManager.class);

    private static final Duration LEASE_TTL = Duration.ofSeconds(15);
    private static final long HEARTBEAT_INTERVAL_MS = 5000;

    private final StringRedisTemplate redisTemplate;
    private final int serverId;
    private final String podId;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicLong currentEpoch = new AtomicLong(0);
    private LeaseCallback callback;

    public MasterLeaseManager(StringRedisTemplate redisTemplate, int serverId, String podId) {
        this.redisTemplate = redisTemplate;
        this.serverId = serverId;
        this.podId = podId;
    }

    public void setCallback(LeaseCallback callback) {
        this.callback = callback;
    }

    /**
     * 尝试获取 lease 并开始心跳。
     */
    public void start() {
        if (tryAcquire()) {
            active.set(true);
            log.info("Acquired master lease, epoch={}", currentEpoch.get());
            if (callback != null) callback.onBecomeActive(currentEpoch.get());
        } else {
            log.info("Lease held by another pod, entering standby mode");
        }
        scheduler.scheduleAtFixedRate(this::heartbeat, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        if (active.get()) {
            releaseLease();
            active.set(false);
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public long getEpoch() {
        return currentEpoch.get();
    }

    private void heartbeat() {
        try {
            if (active.get()) {
                if (!renew()) {
                    log.error("Failed to renew lease, lost master role");
                    active.set(false);
                    if (callback != null) callback.onLostLease();
                }
            } else {
                if (tryAcquire()) {
                    active.set(true);
                    log.info("Acquired master lease (failover), epoch={}", currentEpoch.get());
                    if (callback != null) callback.onBecomeActive(currentEpoch.get());
                }
            }
        } catch (Exception e) {
            log.warn("Lease heartbeat error: {}", e.getMessage());
        }
    }

    private boolean tryAcquire() {
        String leaseKey = leaseKey();
        String epochKey = epochKey();

        // Atomic: set lease if not exists or expired
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(leaseKey, podId, LEASE_TTL);
        if (Boolean.TRUE.equals(acquired)) {
            long epoch = redisTemplate.opsForValue().increment(epochKey);
            currentEpoch.set(epoch);
            return true;
        }

        // Check if lease expired (TTL gone but key still exists due to race)
        Long ttl = redisTemplate.getExpire(leaseKey, TimeUnit.MILLISECONDS);
        if (ttl != null && ttl <= 0) {
            // Try to take over with CAS-like script
            String script = """
                    local current = redis.call('GET', KEYS[1])
                    if current == false or redis.call('TTL', KEYS[1]) <= 0 then
                        redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
                        return redis.call('INCR', KEYS[2])
                    end
                    return 0
                    """;
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(script, Long.class);
            Long result = redisTemplate.execute(redisScript, List.of(leaseKey, epochKey),
                    podId, String.valueOf(LEASE_TTL.toMillis()));
            if (result != null && result > 0) {
                currentEpoch.set(result);
                return true;
            }
        }
        return false;
    }

    private boolean renew() {
        String leaseKey = leaseKey();
        String script = """
                local current = redis.call('GET', KEYS[1])
                if current == ARGV[1] then
                    redis.call('PEXPIRE', KEYS[1], ARGV[2])
                    return 1
                end
                return 0
                """;
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(script, Long.class);
        Long result = redisTemplate.execute(redisScript, List.of(leaseKey),
                podId, String.valueOf(LEASE_TTL.toMillis()));
        return result != null && result == 1;
    }

    private void releaseLease() {
        String leaseKey = leaseKey();
        String script = """
                local current = redis.call('GET', KEYS[1])
                if current == ARGV[1] then
                    redis.call('DEL', KEYS[1])
                    return 1
                end
                return 0
                """;
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(script, Long.class);
        redisTemplate.execute(redisScript, List.of(leaseKey), podId);
    }

    private String leaseKey() {
        return "master_lease:" + serverId;
    }

    private String epochKey() {
        return "master_epoch:" + serverId;
    }

    public interface LeaseCallback {
        void onBecomeActive(long epoch);
        void onLostLease();
    }
}
