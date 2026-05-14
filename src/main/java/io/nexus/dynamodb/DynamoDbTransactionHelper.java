package io.nexus.actor.dynamodb;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * DynamoDB 事务支持。
 * 提供事务写入能力，保证多个操作的原子性。
 */
public class DynamoDbTransactionHelper {

    private final DynamoDbEnhancedAsyncClient enhancedClient;

    public DynamoDbTransactionHelper(DynamoDbEnhancedAsyncClient enhancedClient) {
        this.enhancedClient = enhancedClient;
    }

    /**
     * 事务写入多个 item（最多 100 个）
     */
    public CompletableFuture<Void> transactWrite(String tableName, List<DynamoDbItem> items) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        TransactWriteItemsEnhancedRequest.Builder requestBuilder = TransactWriteItemsEnhancedRequest.builder();
        for (DynamoDbItem item : items) {
            requestBuilder.addPutItem(table, item);
        }

        return enhancedClient.transactWriteItems(requestBuilder.build());
    }

    /**
     * 事务写入：混合 put 和 delete
     */
    public CompletableFuture<Void> transactWriteAndDelete(String tableName,
                                                           List<DynamoDbItem> putItems,
                                                           List<Key> deleteKeys) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        TransactWriteItemsEnhancedRequest.Builder requestBuilder = TransactWriteItemsEnhancedRequest.builder();
        for (DynamoDbItem item : putItems) {
            requestBuilder.addPutItem(table, item);
        }
        for (Key key : deleteKeys) {
            requestBuilder.addDeleteItem(table, key);
        }

        return enhancedClient.transactWriteItems(requestBuilder.build());
    }

    /**
     * 批量写入（非事务，最多 25 个 item per batch）
     */
    public CompletableFuture<Void> batchWrite(String tableName, List<DynamoDbItem> items) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        WriteBatch.Builder<DynamoDbItem> batchBuilder = WriteBatch.builder(DynamoDbItem.class)
                .mappedTableResource(table);
        for (DynamoDbItem item : items) {
            batchBuilder.addPutItem(item);
        }

        BatchWriteItemEnhancedRequest request = BatchWriteItemEnhancedRequest.builder()
                .writeBatches(batchBuilder.build())
                .build();

        return enhancedClient.batchWriteItem(request).thenApply(r -> null);
    }
}
