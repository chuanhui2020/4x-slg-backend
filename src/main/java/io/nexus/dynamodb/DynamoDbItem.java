package io.nexus.actor.dynamodb;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * DynamoDB 通用数据模型基类。
 * 使用 pk (partition key) + sk (sort key) 的单表设计。
 */
@DynamoDbBean
public class DynamoDbItem {

    private String pk;
    private String sk;
    private String data;
    private Long ttl;
    private Long version;

    @DynamoDbPartitionKey
    public String getPk() { return pk; }
    public void setPk(String pk) { this.pk = pk; }

    @DynamoDbSortKey
    public String getSk() { return sk; }
    public void setSk(String sk) { this.sk = sk; }

    public String getData() { return data; }
    public void setData(String data) { this.data = data; }

    public Long getTtl() { return ttl; }
    public void setTtl(Long ttl) { this.ttl = ttl; }

    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
}
