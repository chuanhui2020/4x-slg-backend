package io.nexus.actor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "game")
public class GameProperties {

    private int serverId = 1;
    private String podId = "local";
    private String serverRole = "master";
    private Grpc grpc = new Grpc();
    private Dispatch dispatch = new Dispatch();
    private Player player = new Player();
    private Lease lease = new Lease();
    private RedisConfig redis = new RedisConfig();

    public int getServerId() { return serverId; }
    public void setServerId(int serverId) { this.serverId = serverId; }

    public String getPodId() { return podId; }
    public void setPodId(String podId) { this.podId = podId; }

    public String getServerRole() { return serverRole; }
    public void setServerRole(String serverRole) { this.serverRole = serverRole; }

    public Grpc getGrpc() { return grpc; }
    public void setGrpc(Grpc grpc) { this.grpc = grpc; }

    public Dispatch getDispatch() { return dispatch; }
    public void setDispatch(Dispatch dispatch) { this.dispatch = dispatch; }

    public Player getPlayer() { return player; }
    public void setPlayer(Player player) { this.player = player; }

    public Lease getLease() { return lease; }
    public void setLease(Lease lease) { this.lease = lease; }

    public RedisConfig getRedis() { return redis; }
    public void setRedis(RedisConfig redis) { this.redis = redis; }

    public boolean isMaster() {
        return "master".equalsIgnoreCase(serverRole);
    }

    public static class Grpc {
        private String target = "game-master:9090";
        private int serverPort = 9090;
        private long deadlineMs = 2000;
        private int maxInboundMessageSize = 16 * 1024 * 1024;
        private int keepaliveTimeSeconds = 30;
        private int keepaliveTimeoutSeconds = 5;

        public String getTarget() { return target; }
        public void setTarget(String target) { this.target = target; }
        public int getServerPort() { return serverPort; }
        public void setServerPort(int serverPort) { this.serverPort = serverPort; }
        public long getDeadlineMs() { return deadlineMs; }
        public void setDeadlineMs(long deadlineMs) { this.deadlineMs = deadlineMs; }
        public int getMaxInboundMessageSize() { return maxInboundMessageSize; }
        public void setMaxInboundMessageSize(int maxInboundMessageSize) { this.maxInboundMessageSize = maxInboundMessageSize; }
        public int getKeepaliveTimeSeconds() { return keepaliveTimeSeconds; }
        public void setKeepaliveTimeSeconds(int keepaliveTimeSeconds) { this.keepaliveTimeSeconds = keepaliveTimeSeconds; }
        public int getKeepaliveTimeoutSeconds() { return keepaliveTimeoutSeconds; }
        public void setKeepaliveTimeoutSeconds(int keepaliveTimeoutSeconds) { this.keepaliveTimeoutSeconds = keepaliveTimeoutSeconds; }
    }

    public static class Dispatch {
        private int maxConcurrent = 1024;
        private long defaultTimeoutMs = 5000;

        public int getMaxConcurrent() { return maxConcurrent; }
        public void setMaxConcurrent(int maxConcurrent) { this.maxConcurrent = maxConcurrent; }
        public long getDefaultTimeoutMs() { return defaultTimeoutMs; }
        public void setDefaultTimeoutMs(long defaultTimeoutMs) { this.defaultTimeoutMs = defaultTimeoutMs; }
    }

    public static class Player {
        private int maxPendingEvents = 200;
        private long flushIntervalMs = 100;

        public int getMaxPendingEvents() { return maxPendingEvents; }
        public void setMaxPendingEvents(int maxPendingEvents) { this.maxPendingEvents = maxPendingEvents; }
        public long getFlushIntervalMs() { return flushIntervalMs; }
        public void setFlushIntervalMs(long flushIntervalMs) { this.flushIntervalMs = flushIntervalMs; }
    }

    public static class Lease {
        private long ttlSeconds = 30;
        private long heartbeatIntervalMs = 10000;
        private int maxRenewRetries = 3;
        private long renewRetryBaseMs = 100;

        public long getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(long ttlSeconds) { this.ttlSeconds = ttlSeconds; }
        public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
        public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }
        public int getMaxRenewRetries() { return maxRenewRetries; }
        public void setMaxRenewRetries(int maxRenewRetries) { this.maxRenewRetries = maxRenewRetries; }
        public long getRenewRetryBaseMs() { return renewRetryBaseMs; }
        public void setRenewRetryBaseMs(long renewRetryBaseMs) { this.renewRetryBaseMs = renewRetryBaseMs; }
    }

    public static class RedisConfig {
        private Rpc rpc = new Rpc();
        private Location location = new Location();

        public Rpc getRpc() { return rpc; }
        public void setRpc(Rpc rpc) { this.rpc = rpc; }
        public Location getLocation() { return location; }
        public void setLocation(Location location) { this.location = location; }

        public static class Rpc {
            private int maxPendingAsks = 4096;
            private long cleanupIntervalSeconds = 30;

            public int getMaxPendingAsks() { return maxPendingAsks; }
            public void setMaxPendingAsks(int maxPendingAsks) { this.maxPendingAsks = maxPendingAsks; }
            public long getCleanupIntervalSeconds() { return cleanupIntervalSeconds; }
            public void setCleanupIntervalSeconds(long cleanupIntervalSeconds) { this.cleanupIntervalSeconds = cleanupIntervalSeconds; }
        }

        public static class Location {
            private long ttlSeconds = 120;
            private long renewIntervalSeconds = 40;

            public long getTtlSeconds() { return ttlSeconds; }
            public void setTtlSeconds(long ttlSeconds) { this.ttlSeconds = ttlSeconds; }
            public long getRenewIntervalSeconds() { return renewIntervalSeconds; }
            public void setRenewIntervalSeconds(long renewIntervalSeconds) { this.renewIntervalSeconds = renewIntervalSeconds; }
        }
    }
}
