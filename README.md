# Nexus Actor Framework

[![Java](https://img.shields.io/badge/Java-25-orange)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.0.0-green)](https://spring.io/projects/spring-boot)
[![Pekko](https://img.shields.io/badge/Apache%20Pekko-1.1.3-blue)](https://pekko.apache.org/)
[![License](https://img.shields.io/badge/License-Private-red)]()

基于 Apache Pekko Typed 的游戏服务器 Actor 框架，专为 4X-SLG 类型游戏设计。提供完整的 Actor 生命周期管理、AOI 视野系统、跨服通信和 Master/Slave 部署模型。

---

## 技术栈

<table>
<tr><th width="160">组件</th><th width="160">版本</th><th>用途</th></tr>
<tr><td>Java</td><td>25</td><td>Virtual Threads + Sealed Interface + Record</td></tr>
<tr><td>Spring Boot</td><td>4.0.0</td><td>DI + Auto Configuration</td></tr>
<tr><td>Apache Pekko Typed</td><td>1.1.3</td><td>Actor 模型核心</td></tr>
<tr><td>Redis (Lettuce)</td><td>Spring Boot 管理</td><td>场景缓存 + Stream RPC + 视野推送</td></tr>
<tr><td>AWS DynamoDB</td><td>SDK 2.31.1</td><td>玩家/公会持久化</td></tr>
<tr><td>Jackson</td><td>Spring Boot 管理</td><td>RPC 消息序列化</td></tr>
</table>

---

## 架构总览

<table>
<tr>
<th width="50%">Master Pod (1个)</th>
<th width="50%">Slave Pod (N个, HPA)</th>
</tr>
<tr>
<td>

```
ActorSystem
├── PlayerActor-* (连接到本Pod的玩家)
├── GuildActor-* (per guild, TTL缓存)
└── MapSceneActor
    ├── AreaActor-1~N (per area, 75x75格子)
    ├── MarchSupervisor
    │   └── MarchActor-* (per行军)
    ├── RallySupervisor
    │   └── RallyActor-* (per集结)
    └── EventSupervisor
        ├── WorldBossActor-*
        └── GuildWarActor-*
```

</td>
<td>

```
ActorSystem
└── PlayerActor-* (连接到本Pod的玩家)
    ├── 读操作本地处理
    ├── 写操作RPC到Master
    └── 视野推送: 消费Redis Stream
```

</td>
</tr>
</table>

**通信路径：**

<table>
<tr><th width="160">方向</th><th width="200">方式</th><th>Stream Key</th></tr>
<tr><td>Slave → Master</td><td>Redis Stream</td><td><code>rpc:master:{serverId}</code></td></tr>
<tr><td>Master → Slave</td><td>Redis Stream</td><td><code>rpc:pod:{podId}</code></td></tr>
<tr><td>视野推送</td><td>Redis Stream</td><td><code>scene:change:{serverId}:{areaId}</code></td></tr>
</table>

---

## 核心特性

### 路由决策

所有 Actor 间通信统一走 `ActorRouter`，根据目标地址自动选择本地调用或远程 RPC。跨服战和 Master/Slave 通信共用同一套代码。

### AOI 视野系统

地图 1000×1000 格子，划分为 196 个区域（每个 75×75，14×14 布局）。支持视口订阅、移动 diff 算、Grid 级精确过滤。Master 直接 Actor 通知，Slave 通过 Redis Stream 消费。

### 行军系统（无 Tick）

MarchActor 不使用定时 tick，出发时计算完整路径和到达时间，设置单次定时器。支持召回、拦截、加速等中断操作。

### 集结系统（状态机）

RallyActor 使用 Pekko Typed Behavior 切换实现多阶段状态机：
`WAITING → MARCHING → BATTLING → RETURNING → DISMISSED`

### 持久化策略

<table>
<tr><th width="180">Actor</th><th width="160">存储</th><th>策略</th></tr>
<tr><td>PlayerActor</td><td>DynamoDB</td><td>已有逻辑</td></tr>
<tr><td>GuildActor</td><td>DynamoDB + Redis</td><td>写同步 DDB，Redis 供读</td></tr>
<tr><td>AreaActor</td><td>Redis</td><td>1s 异步批量</td></tr>
<tr><td>MarchActor</td><td>无</td><td>短生命周期</td></tr>
<tr><td>RallyActor / WorldBoss</td><td>Redis</td><td>定期快照</td></tr>
</table>

---

## 扩展点

框架通过 Behavior 接口留出业务扩展点：

<table>
<tr><th width="220">接口</th><th>职责</th></tr>
<tr><td><code>AreaBehavior</code></td><td>区域 tick、攻击处理、初始对象生成</td></tr>
<tr><td><code>MarchBehavior</code></td><td>到达时间计算、战斗结果、路线信息</td></tr>
<tr><td><code>RallyBehavior</code></td><td>加入条件、出发/到达逻辑</td></tr>
<tr><td><code>GuildBehavior</code></td><td>加入/踢出条件、捐献、人数上限</td></tr>
<tr><td><code>PlayerMessageHandler</code></td><td>请求处理、视野更新构建</td></tr>
<tr><td><code>PlayerSession</code></td><td>网络层抽象</td></tr>
</table>

---

## 快速开始

```java
// 创建框架实例
var rpcTransport = new RedisRpcTransport(redisTemplate, serializer);
var framework = ActorFramework.create(rpcTransport);

// 启动场景（仅 Master）
if (framework.getRoleProvider().isMaster()) {
    var sceneRef = framework.getSystem().systemActorOf(
        MapSceneActor.create(registry, router, cacheProvider,
            streamPublisher, serializer, serverId,
            areaBehavior, marchBehavior, rallyBehavior),
        "map-scene");
    sceneRef.tell(new SceneMessage.Init(196));
}

// 玩家连接时创建 PlayerActor
var playerRef = framework.getSystem().systemActorOf(
    PlayerActor.create(playerId, session, isMaster,
        router, streamConsumer, serverId, handler, aoiCalc),
    "player-" + playerId);
framework.getRegistry().register(ActorType.PLAYER, playerId, playerRef);
```

---

## 部署模型

<table>
<tr><th width="100">角色</th><th width="100">副本数</th><th width="140">环境变量</th><th width="140">资源</th><th>策略</th></tr>
<tr><td>Master</td><td>1</td><td><code>SERVER_ROLE=master</code><br><code>SERVER_ID=1</code></td><td>CPU 2-4<br>Mem 4-8Gi</td><td>Recreate</td></tr>
<tr><td>Slave</td><td>2-5</td><td><code>SERVER_ROLE=slave</code><br><code>SERVER_ID=1</code></td><td>CPU 1-2<br>Mem 2-4Gi</td><td>HPA by CPU 70%</td></tr>
</table>

---

## 构建

```bash
mvn compile    # 编译
mvn package    # 打包
mvn install    # 安装到本地仓库
```

## 引用

```xml
<dependency>
    <groupId>io.nexus</groupId>
    <artifactId>actor-framework</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
