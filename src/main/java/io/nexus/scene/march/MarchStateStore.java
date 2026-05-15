package io.nexus.actor.scene.march;

import io.nexus.actor.serialize.RpcSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;

/**
 * 行军状态持久化。
 * 活跃行军存入 Redis Hash，Master 重启时可恢复。
 */
public class MarchStateStore {

    private static final Logger log = LoggerFactory.getLogger(MarchStateStore.class);

    private final StringRedisTemplate redisTemplate;
    private final RpcSerializer serializer;
    private final int serverId;

    public MarchStateStore(StringRedisTemplate redisTemplate, RpcSerializer serializer, int serverId) {
        this.redisTemplate = redisTemplate;
        this.serializer = serializer;
        this.serverId = serverId;
    }

    public void save(MarchSnapshot snapshot) {
        String hashKey = hashKey();
        String field = String.valueOf(snapshot.marchId());
        byte[] data = serializer.serialize(snapshot);
        String encoded = Base64.getEncoder().encodeToString(data);
        redisTemplate.opsForHash().put(hashKey, field, encoded);
    }

    public void remove(long marchId) {
        redisTemplate.opsForHash().delete(hashKey(), String.valueOf(marchId));
    }

    public List<MarchSnapshot> loadAll() {
        String hashKey = hashKey();
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(hashKey);
        List<MarchSnapshot> result = new ArrayList<>();
        for (var entry : entries.values()) {
            try {
                byte[] data = Base64.getDecoder().decode((String) entry);
                MarchSnapshot snapshot = serializer.deserialize(data, MarchSnapshot.class);
                if (snapshot != null) {
                    result.add(snapshot);
                }
            } catch (Exception e) {
                log.warn("Failed to deserialize march snapshot: {}", e.getMessage());
            }
        }
        return result;
    }

    private String hashKey() {
        return "march:active:" + serverId;
    }

    public record MarchSnapshot(
            long marchId,
            long playerId,
            List<io.nexus.actor.view.GridId> path,
            float speed,
            long targetId,
            int type,
            Object troops,
            long departTime,
            long expectedArrivalTime,
            String replyPodId
    ) {}
}
