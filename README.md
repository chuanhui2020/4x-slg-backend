# Nexus Actor Framework

[![Java](https://img.shields.io/badge/Java-25-orange)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.0.0-green)](https://spring.io/projects/spring-boot)
[![Pekko](https://img.shields.io/badge/Apache%20Pekko-1.1.3-blue)](https://pekko.apache.org/)
[![License](https://img.shields.io/badge/License-Private-red)]()

基于 Apache Pekko Typed 的游戏服务器 Actor 框架，专为 4X-SLG 类型游戏设计。提供完整的 Actor 生命周期管理、AOI 视野系统、跨服通信和 Master/Slave 部署模型。

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Java | 25 | Virtual Threads + Sealed Interface + Record |
| Spring Boot | 4.0.0 | DI + Auto Configuration |
| Apache Pekko Typed | 1.1.3 | Actor 模型核心 |
| Redis (Lettuce) | Spring Boot 管理 | 场景缓存 + Stream RPC + 视野推送 |
| AWS DynamoDB | SDK 2.31.1 | 玩家/公会持久化 |
| Jackson | Spring Boot 管理 | RPC 消息序列化 |

## 架构总览

```
┌───────────────────────────────────────────────────────────────┐
│                      Master Pod (1个)                          │
│                                                               │
│  ActorSystem                                                  │
│  ├── PlayerActor-* (连接到本Pod的玩家)                        │
│  ├── GuildActor-* (per guild, TTL缓存)                        │
│  └── MapSceneActor                                            │
│        ├── AreaActor-1~N (per area, 75x75格子)                │
│        ├── MarchSupervisor -> MarchActor-* (per行军)          │
│        ├── RallySupervisor -> RallyActor-* (per集结)          │
│        └── EventSupervisor                                    │
│              ├── WorldBossActor-*                              │
│              └── GuildWarActor-*                               │
└───────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────┐
│                    Slave Pod (N个, HPA)                        │
│                                                               │
│  ActorSystem                                                  │
│  └── PlayerActor-* (连接到本Pod的玩家)                        │
│        - 读操作本地处理                                       │
│        - 写操作RPC到Master                                    │
│        - 视野推送: 消费Redis Stream                           │
└───────────────────────────────────────────────────────────────┘

通信路径:
  Slave  -> Master:  Redis Stream (rpc:master:{serverId})
  Master -> Slave:   Redis Stream (rpc:pod:{podId})
  视野推送:          Redis Stream (scene:change:{serverId}:{areaId})
```

## 核心特性

### 路由决策

所有 Actor 间通信统一走 `ActorRouter`，根据目标地址自动选择本地调用或远程 RPC。跨服战和 Master/Slave 通信共用同一套代码。

### AOI 视野系统

地图 1000×1000 格子，划分为 169 个区域（每个 75×75）。支持视口订阅、移动 diff 算、Grid 级精确过滤。Master 直接 Actor 通知，Slave 通过 Redis Stream 消费。

### 行军系统（无 Tick）

MarchActor 不使用定时 tick，出发时计算完整路径和到达时间，设置单次定时器。支持召回、拦截、加速等中断操作。

### 集结系统（状态机）

RallyActor 使用 Pekko Typed Behavior 切换实现多阶段状态机：
`WAITING → MARCHING → BATTLING → RETURNING → DISMISSED`

### 持久化策略

| Actor | 存储 | 策略 |
|-------|------|------|
| PlayerActor | DynamoDB | 已有逻辑 |
| GuildActor | DynamoDB + Redis | 写同步 DDB，Redis 供读 |
| AreaActor | Redis | 1s 异步批量 |
| MarchActor | 无 | 短生命周期 |
| RallyActor / WorldBoss | Redis | 定期快照 |

## 扩展点

框架通过 Behavior 接口留出业务扩展点：

| 接口 | 职责 |
|------|------|
| `AreaBehavior` | 区域 tick、攻击处理、初始对象生成 |
| `MarchBehavior` | 到达时间计算、战斗结果、路线信息 |
| `RallyBehavior` | 加入条件、出发/到达逻辑 |
| `GuildBehavior` | 加入/踢出条件、捐献、人数上限 |
| `PlayerMessageHandler` | 请求处理、视野更新构建 |
| `PlayerSession` | 网络层抽象 |

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
    sceneRef.tell(new SceneMessage.Init(169));
}

// 玩家连接时创建 PlayerActor
var playerRef = framework.getSystem().systemActorOf(
    PlayerActor.create(playerId, session, isMaster,
        router, streamConsumer, serverId, handler, aoiCalc),
    "player-" + playerId);
framework.getRegistry().register(ActorType.PLAYER, playerId, playerRef);
```

## 部署模型

```yaml
# Master: 固定 1 Pod, Recreate 策略
game-server-master:
  replicas: 1
  env: SERVER_ROLE=master, SERVER_ID=1
  resources: cpu 2-4, memory 4-8Gi

# Slave: HPA 弹性伸缩
game-server-slave:
  replicas: 2-5 (HPA by CPU 70%)
  env: SERVER_ROLE=slave, SERVER_ID=1
  resources: cpu 1-2, memory 2-4Gi
```

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
