package io.nexus.actor.dynamodb;

import io.nexus.actor.persistence.PersistenceProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * DynamoDB 实现的 PersistenceProvider。
 * 使用 Enhanced Client 的异步 API + 单表设计（pk/sk/data）。
 *
 * @param <K> 业务 key 类型
 * @param <V> 业务 value 类型
 */
public class DynamoDbPersistenceProvider<K, V> implements PersistenceProvider<K, V> {

    private final DynamoDbAsyncTable<DynamoDbItem> table;
    private final DynamoDbEnhancedAsyncClient enhancedClient;
    private final DynamoDbKeyStrategy<K> keyStrategy;
    private final DynamoDbValueSerializer<V> valueSerializer;
    private final String tableName;

    public DynamoDbPersistenceProvider(DynamoDbEnhancedAsyncClient enhancedClient,
                                       String tableName,
                                       DynamoDbKeyStrategy<K> keyStrategy,
                                       DynamoDbValueSerializer<V> valueSerializer) {
        this.enhancedClient = enhancedClient;
        this.tableName = tableName;
        this.keyStrategy = keyStrategy;
        this.valueSerializer = valueSerializer;
        this.table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));
    }

    @Override
    public CompletableFuture<V> load(K key) {
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

    @Override
    public CompletableFuture<Void> save(K key, V value) {
        DynamoDbItem item = new DynamoDbItem();
        item.setPk(keyStrategy.toPartitionKey(key));
        item.setSk(keyStrategy.toSortKey(key));
        item.setData(valueSerializer.serialize(value));
        item.setVersion(System.currentTimeMillis());
        return table.putItem(item).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> delete(K key) {
        Key dynamoKey = Key.builder()
                .partitionValue(keyStrategy.toPartitionKey(key))
                .sortValue(keyStrategy.toSortKey(key))
                .build();
        return table.deleteItem(dynamoKey).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> batchSave(Map<K, V> entries) {
        if (entries.isEmpty()) return CompletableFuture.completedFuture(null);

        WriteBatch.Builder<DynamoDbItem> batchBuilder = WriteBatch.builder(DynamoDbItem.class)
                .mappedTableResource(table);

        for (var entry : entries.entrySet()) {
            DynamoDbItem item = new DynamoDbItem();
            item.setPk(keyStrategy.toPartitionKey(entry.getKey()));
            item.setSk(keyStrategy.toSortKey(entry.getKey()));
            item.setData(valueSerializer.serialize(entry.getValue()));
            item.setVersion(System.currentTimeMillis());
            batchBuilder.addPutItem(item);
        }

        BatchWriteItemEnhancedRequest request = BatchWriteItemEnhancedRequest.builder()
                .writeBatches(batchBuilder.build())
                .build();

        return enhancedClient.batchWriteItem(request).thenApply(r -> null);
    }

    public DynamoDbAsyncTable<DynamoDbItem> getTable() {
        return table;
    }

    public String getTableName() {
        return tableName;
    }
}
