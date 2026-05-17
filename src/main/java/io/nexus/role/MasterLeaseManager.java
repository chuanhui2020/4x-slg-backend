package io.nexus.actor.role;

import io.nexus.metrics.GameMetrics;
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

public class MasterLeaseManager {

    private static final Logger log = LoggerFactory.getLogger(MasterLeaseManager.class);

    private final StringRedisTemplate redisTemplate;
    private final int serverId;
    private final String podId;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2,
            r -> new Thread(r, "master-lease-" + Thread.currentThread().threadId()));
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicLong currentEpoch = new AtomicLong(0);
    private final GameMetrics metrics;
    private final Duration leaseTtl;
    private final long heartbeatIntervalMs;
    private final int maxRenewRetries;
    private final long renewRetryBaseMs;
    private LeaseCallback callback;

    private static final String ACQUIRE_SCRIPT = """
            local leaseKey = KEYS[1]
            local epochKey = KEYS[2]
            local podId = ARGV[1]
            local ttlMs = tonumber(ARGV[2])
            local current = redis.call('GET', leaseKey)
            if current == false then
                redis.call('SET', leaseKey, podId, 'PX', ttlMs)
                return redis.call('INCR', epochKey)
            end
            local remainTtl = redis.call('PTTL', leaseKey)
            if remainTtl <= 0 then
                redis.call('SET', leaseKey, podId, 'PX', ttlMs)
                return redis.call('INCR', epochKey)
            end
            return 0
            """;

    private static final String RENEW_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            if current == ARGV[1] then
                redis.call('PEXPIRE', KEYS[1], ARGV[2])
                return 1
            end
            return 0
            """;

    private static final String RELEASE_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            if current == ARGV[1] then
                redis.call('DEL', KEYS[1])
                return 1
            end
            return 0
            """;

    public MasterLeaseManager(StringRedisTemplate redisTemplate, int serverId, String podId,
                               GameMetrics metrics, long leaseTtlSeconds, long heartbeatIntervalMs,
                               int maxRenewRetries, long renewRetryBaseMs) {
        this.redisTemplate = redisTemplate;
        this.serverId = serverId;
        this.podId = podId;
        this.metrics = metrics;
        this.leaseTtl = Duration.ofSeconds(leaseTtlSeconds);
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.maxRenewRetries = maxRenewRetries;
        this.renewRetryBaseMs = renewRetryBaseMs;
    }

    public void setCallback(LeaseCallback callback) {
        this.callback = callback;
    }

    public void start() {
        if (tryAcquire()) {
            active.set(true);
            log.info("Acquired master lease, epoch={}", currentEpoch.get());
            if (metrics != null) metrics.leaseEpoch().set(currentEpoch.get());
            if (callback != null) callback.onBecomeActive(currentEpoch.get());
        } else {
            log.info("Lease held by another pod, entering standby mode");
        }
        scheduler.scheduleAtFixedRate(this::heartbeat, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
                if (!renewWithRetry()) {
                    log.error("Failed to renew lease after {} retries, lost master role", maxRenewRetries);
                    active.set(false);
                    if (metrics != null) metrics.leaseRenewalFailureCounter().increment();
                    if (callback != null) callback.onLostLease();
                }
            } else {
                if (tryAcquire()) {
                    active.set(true);
                    log.info("Acquired master lease (failover), epoch={}", currentEpoch.get());
                    if (metrics != null) metrics.leaseEpoch().set(currentEpoch.get());
                    if (callback != null) callback.onBecomeActive(currentEpoch.get());
                }
            }
        } catch (Exception e) {
            log.warn("Lease heartbeat error: {}", e.getMessage());
        }
    }

    private boolean tryAcquire() {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(ACQUIRE_SCRIPT, Long.class);
        Long result = redisTemplate.execute(redisScript,
                List.of(leaseKey(), epochKey()),
                podId, String.valueOf(leaseTtl.toMillis()));
        if (result != null && result > 0) {
            currentEpoch.set(result);
            return true;
        }
        return false;
    }

    private boolean renewWithRetry() {
        for (int attempt = 0; attempt < maxRenewRetries; attempt++) {
            var sample = (metrics != null) ? io.micrometer.core.instrument.Timer.start() : null;
            try {
                if (renew()) {
                    if (sample != null && metrics != null) sample.stop(metrics.leaseRenewalTimer());
                    return true;
                }
            } catch (Exception e) {
                log.warn("Lease renewal attempt {} failed: {}", attempt + 1, e.getMessage());
            }
            if (attempt < maxRenewRetries - 1) {
                try {
                    Thread.sleep(renewRetryBaseMs * (1L << attempt));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    private boolean renew() {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(RENEW_SCRIPT, Long.class);
        Long result = redisTemplate.execute(redisScript, List.of(leaseKey()),
                podId, String.valueOf(leaseTtl.toMillis()));
        return result != null && result == 1;
    }

    private void releaseLease() {
        try {
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(RELEASE_SCRIPT, Long.class);
            redisTemplate.execute(redisScript, List.of(leaseKey()), podId);
            log.info("Master lease released");
        } catch (Exception e) {
            log.warn("Failed to release lease on shutdown: {}", e.getMessage());
        }
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
