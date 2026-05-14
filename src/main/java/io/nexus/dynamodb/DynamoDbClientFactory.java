package io.nexus.actor.dynamodb;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.net.URI;

/**
 * DynamoDB 客户端工厂。
 * 创建 DynamoDbEnhancedAsyncClient 实例。
 */
public class DynamoDbClientFactory {

    private final DynamoDbAsyncClient asyncClient;
    private final DynamoDbEnhancedAsyncClient enhancedClient;

    public DynamoDbClientFactory(String region) {
        this.asyncClient = DynamoDbAsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        this.enhancedClient = DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(asyncClient)
                .build();
    }

    public DynamoDbClientFactory(String region, String endpoint) {
        this.asyncClient = DynamoDbAsyncClient.builder()
                .region(Region.of(region))
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        this.enhancedClient = DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(asyncClient)
                .build();
    }

    public DynamoDbClientFactory(DynamoDbAsyncClient asyncClient) {
        this.asyncClient = asyncClient;
        this.enhancedClient = DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(asyncClient)
                .build();
    }

    public DynamoDbAsyncClient getAsyncClient() {
        return asyncClient;
    }

    public DynamoDbEnhancedAsyncClient getEnhancedClient() {
        return enhancedClient;
    }

    public void close() {
        asyncClient.close();
    }
}
