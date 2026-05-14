package io.nexus.actor.dynamodb;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * DynamoDB 值序列化接口。
 * 定义如何将业务对象序列化为 JSON 字符串存储到 DynamoDB。
 */
public interface DynamoDbValueSerializer<V> {

    String serialize(V value);

    V deserialize(String data);

    /**
     * 基于 Jackson 的默认实现
     */
    static <V> DynamoDbValueSerializer<V> jackson(ObjectMapper mapper, Class<V> type) {
        return new DynamoDbValueSerializer<>() {
            @Override
            public String serialize(V value) {
                try {
                    return mapper.writeValueAsString(value);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize " + type.getName(), e);
                }
            }

            @Override
            public V deserialize(String data) {
                try {
                    return mapper.readValue(data, type);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to deserialize " + type.getName(), e);
                }
            }
        };
    }
}
