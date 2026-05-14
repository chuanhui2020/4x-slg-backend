package io.nexus.actor.dynamodb;

/**
 * 常用的 DynamoDB Key 策略实现。
 */
public final class DynamoDbKeyStrategies {

    private DynamoDbKeyStrategies() {}

    /**
     * 玩家数据 key 策略：pk = "PLAYER#{playerId}", sk = "PROFILE"
     */
    public static DynamoDbKeyStrategy<Long> player() {
        return player("PROFILE");
    }

    /**
     * 玩家数据 key 策略：pk = "PLAYER#{playerId}", sk = 自定义
     */
    public static DynamoDbKeyStrategy<Long> player(String sortKey) {
        return new DynamoDbKeyStrategy<>() {
            @Override
            public String toPartitionKey(Long key) {
                return "PLAYER#" + key;
            }

            @Override
            public String toSortKey(Long key) {
                return sortKey;
            }

            @Override
            public Long fromKeys(String pk, String sk) {
                return Long.parseLong(pk.substring("PLAYER#".length()));
            }
        };
    }

    /**
     * 公会数据 key 策略：pk = "GUILD#{guildId}", sk = "INFO"
     */
    public static DynamoDbKeyStrategy<Long> guild() {
        return guild("INFO");
    }

    /**
     * 公会数据 key 策略：pk = "GUILD#{guildId}", sk = 自定义
     */
    public static DynamoDbKeyStrategy<Long> guild(String sortKey) {
        return new DynamoDbKeyStrategy<>() {
            @Override
            public String toPartitionKey(Long key) {
                return "GUILD#" + key;
            }

            @Override
            public String toSortKey(Long key) {
                return sortKey;
            }

            @Override
            public Long fromKeys(String pk, String sk) {
                return Long.parseLong(pk.substring("GUILD#".length()));
            }
        };
    }

    /**
     * 公会成员 key 策略：pk = "GUILD#{guildId}", sk = "MEMBER#{playerId}"
     */
    public static DynamoDbKeyStrategy<GuildMemberKey> guildMember() {
        return new DynamoDbKeyStrategy<>() {
            @Override
            public String toPartitionKey(GuildMemberKey key) {
                return "GUILD#" + key.guildId();
            }

            @Override
            public String toSortKey(GuildMemberKey key) {
                return "MEMBER#" + key.playerId();
            }

            @Override
            public GuildMemberKey fromKeys(String pk, String sk) {
                long guildId = Long.parseLong(pk.substring("GUILD#".length()));
                long playerId = Long.parseLong(sk.substring("MEMBER#".length()));
                return new GuildMemberKey(guildId, playerId);
            }
        };
    }

    /**
     * 服务器级数据 key 策略：pk = "SERVER#{serverId}", sk = 自定义前缀 + id
     */
    public static DynamoDbKeyStrategy<ServerDataKey> serverData(String prefix) {
        return new DynamoDbKeyStrategy<>() {
            @Override
            public String toPartitionKey(ServerDataKey key) {
                return "SERVER#" + key.serverId();
            }

            @Override
            public String toSortKey(ServerDataKey key) {
                return prefix + "#" + key.dataId();
            }

            @Override
            public ServerDataKey fromKeys(String pk, String sk) {
                int serverId = Integer.parseInt(pk.substring("SERVER#".length()));
                long dataId = Long.parseLong(sk.substring(prefix.length() + 1));
                return new ServerDataKey(serverId, dataId);
            }
        };
    }

    /**
     * 通用 String key 策略：pk = key, sk = 固定值
     */
    public static DynamoDbKeyStrategy<String> simpleString(String fixedSortKey) {
        return new DynamoDbKeyStrategy<>() {
            @Override
            public String toPartitionKey(String key) {
                return key;
            }

            @Override
            public String toSortKey(String key) {
                return fixedSortKey;
            }

            @Override
            public String fromKeys(String pk, String sk) {
                return pk;
            }
        };
    }

    public record GuildMemberKey(long guildId, long playerId) {}

    public record ServerDataKey(int serverId, long dataId) {}
}
