package io.nexus.actor.dynamodb;

/**
 * DynamoDB 键策略接口。
 * 定义如何将业务 key 映射为 DynamoDB 的 pk/sk。
 */
public interface DynamoDbKeyStrategy<K> {

    String toPartitionKey(K key);

    String toSortKey(K key);

    K fromKeys(String pk, String sk);
}
