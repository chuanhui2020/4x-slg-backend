package io.nexus.actor.dynamodb;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * DynamoDB 批量加载辅助类。
 * 用于 Actor 启动时从 DynamoDB 批量加载数据。
 */
public class DynamoDbBatchLoader<K, V> {

    private final DynamoDbEnhancedAsyncClient enhancedClient;
    private final String tableName;
    private final DynamoDbKeyStrategy<K> keyStrategy;
    private final DynamoDbValueSerializer<V> valueSerializer;

    public DynamoDbBatchLoader(DynamoDbEnhancedAsyncClient enhancedClient, String tableName,
                                DynamoDbKeyStrategy<K> keyStrategy, DynamoDbValueSerializer<V> valueSerializer) {
        this.enhancedClient = enhancedClient;
        this.tableName = tableName;
        this.keyStrategy = keyStrategy;
        this.valueSerializer = valueSerializer;
    }

    /**
     * 按 partition key 加载所有数据
     */
    public CompletableFuture<List<V>> loadByPartitionKey(String pk) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        QueryEnhancedRequest request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.keyEqualTo(Key.builder().partitionValue(pk).build()))
                .build();

        List<DynamoDbItem> items = new ArrayList<>();
        return table.query(request)
                .subscribe(page -> items.addAll(page.items()))
                .thenApply(v -> items.stream()
                        .filter(item -> item.getData() != null)
                        .map(item -> valueSerializer.deserialize(item.getData()))
                        .collect(Collectors.toList()));
    }

    /**
     * 按 partition key + sort key 前缀加载
     */
    public CompletableFuture<List<V>> loadByPrefix(String pk, String skPrefix) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        QueryEnhancedRequest request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.sortBeginsWith(
                        Key.builder().partitionValue(pk).sortValue(skPrefix).build()))
                .build();

        List<DynamoDbItem> items = new ArrayList<>();
        return table.query(request)
                .subscribe(page -> items.addAll(page.items()))
                .thenApply(v -> items.stream()
                        .filter(item -> item.getData() != null)
                        .map(item -> valueSerializer.deserialize(item.getData()))
                        .collect(Collectors.toList()));
    }

    /**
     * 加载单个 item
     */
    public CompletableFuture<V> loadOne(K key) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        Key dynamoKey = Key.builder()
                .partitionValue(keyStrategy.toPartitionKey(key))
                .sortValue(keyStrategy.toSortKey(key))
                .build();

        return table.getItem(dynamoKey)
                .thenApply(item -> {
                    if (item == null || item.getData() == null) return null;
                    return valueSerializer.deserialize(item.getData());
                });
    }
}
