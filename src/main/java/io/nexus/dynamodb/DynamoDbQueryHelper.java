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

/**
 * DynamoDB 查询辅助类。
 * 支持按 partition key 查询所有 sort key 下的数据。
 */
public class DynamoDbQueryHelper {

    private final DynamoDbEnhancedAsyncClient enhancedClient;

    public DynamoDbQueryHelper(DynamoDbEnhancedAsyncClient enhancedClient) {
        this.enhancedClient = enhancedClient;
    }

    /**
     * 查询某个 partition key 下的所有 item
     */
    public CompletableFuture<List<DynamoDbItem>> queryByPartitionKey(String tableName, String pk) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        QueryEnhancedRequest request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.keyEqualTo(Key.builder().partitionValue(pk).build()))
                .build();

        List<DynamoDbItem> results = new ArrayList<>();
        return table.query(request)
                .subscribe(page -> results.addAll(page.items()))
                .thenApply(v -> results);
    }

    /**
     * 查询某个 partition key 下，sort key 以指定前缀开头的 item
     */
    public CompletableFuture<List<DynamoDbItem>> queryByPrefix(String tableName, String pk, String skPrefix) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        QueryEnhancedRequest request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.sortBeginsWith(
                        Key.builder().partitionValue(pk).sortValue(skPrefix).build()))
                .build();

        List<DynamoDbItem> results = new ArrayList<>();
        return table.query(request)
                .subscribe(page -> results.addAll(page.items()))
                .thenApply(v -> results);
    }

    /**
     * 查询某个 partition key 下，sort key 在指定范围内的 item
     */
    public CompletableFuture<List<DynamoDbItem>> queryByRange(String tableName, String pk, String skFrom, String skTo) {
        DynamoDbAsyncTable<DynamoDbItem> table = enhancedClient.table(tableName, TableSchema.fromBean(DynamoDbItem.class));

        QueryEnhancedRequest request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.sortBetween(
                        Key.builder().partitionValue(pk).sortValue(skFrom).build(),
                        Key.builder().partitionValue(pk).sortValue(skTo).build()))
                .build();

        List<DynamoDbItem> results = new ArrayList<>();
        return table.query(request)
                .subscribe(page -> results.addAll(page.items()))
                .thenApply(v -> results);
    }
}
