# Nexus Actor Framework

基于 Apache Pekko Typed 的游戏服务器 Actor 框架，专为 4X-SLG 类型游戏设计。提供完整的 Actor 生命周期管理、AOI 视野系统、跨服通信、Master/Slave 部署模型。

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
┌─────────────────────────────────────────────────────────────────┐
│                        Master Pod (1个)                          │
│                                                                 │
│  ActorSystem                                                    │
│  ├── PlayerActor-* (连接到本 Pod 的玩家)                          │
│  ├── GuildActor-* (per guild, TTL 缓存)                         │
│  └── MapSceneActor                                              │
│        ├── AreaActor-1 ~ N (per area, 75x75 格子)               │
│        ├── MarchSupervisor → MarchActor-* (per 行军)            │
│        ├── RallySupervisor → RallyActor-* (per 集结)            │
│        └── EventSupervisor                                      │
│              ├── WorldBossActor-*                                │
│              └── GuildWarActor-*                                 │
│                                                                 │
│  通信：Actor 间直接 tell（零延迟）                                 │
│  持久化：AreaActor → Redis, GuildActor → DynamoDB               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     Slave Pod (N个, HPA)                         │
│                                                                 │
│  ActorSystem                                                    │
│  └── PlayerActor-* (连接到本 Pod 的玩家)                          │
│        - 读操作本地处理                                           │
│        - 写操作 RPC 到 Master                                    │
│        - 视野推送：消费 Redis Stream                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

通信路径：
  slave → master:  Redis Stream (rpc:master:{serverId})
  master → slave:  Redis Stream (rpc:pod:{podId})
  视野推送:        Redis Stream (scene:change:{serverId}:{areaId})
```

## 包结构

```
io.nexus.actor/
│
├── ActorFramework              框架启动入口，组装所有组件
│
├── address/                    Actor 寻址
│   ├── ActorAddress            record(type, id, serverId) — Actor 的全局唯一地址
│   └── ActorType               枚举：PLAYER, GUILD, AREA, MARCH_SUPERVISOR...
│
├── role/                       Master/Slave 角色判定
│   ├── ServerRole              enum: MASTER, SLAVE
│   └── ServerRoleProvider      从环境变量 SERVER_ROLE / SERVER_ID 读取
│
├── registry/                   Actor 注册表
│   └── ActorRegistry           ConcurrentHashMap<type:id, ActorRef>
│
├── router/                     统一路由层
│   ├── ActorInvoker            interface: tell() / ask()
│   ├── ActorRouter             自动选择 local/remote（核心路由决策）
│   ├── LocalActorInvoker       本地 ActorRef.tell / AskPattern
│   └── RemoteActorInvoker      委托 RpcTransport 远程调用
│
├── rpc/                        RPC 协议层
│   ├── ActorEnvelope           record(target, message, replyTo)
│   ├── RpcTransport            interface: sendTell / sendAsk
│   └── RpcMessageHandler       远程消息接收 → 反序列化 → 路由到本地 Actor
│
├── serialize/                  序列化
│   ├── RpcSerializer           interface
│   └── JacksonRpcSerializer    Jackson 实现（带多态类型信息）
│
├── persistence/                持久化接口层
│   ├── PersistenceProvider     interface: load/save/delete/batchSave
│   ├── CacheProvider           interface: get/put/putAll/remove
│   ├── StreamPublisher         interface: publish
│   └── StreamConsumer          interface: subscribe/unsubscribe
│
├── redis/                      Redis 默认实现
│   ├── RedisCacheProvider      CacheProvider 实现
│   ├── RedisStreamPublisher    StreamPublisher 实现 (XADD)
│   ├── RedisStreamConsumer     StreamConsumer 实现 (XREAD, virtual thread)
│   ├── RedisRpcTransport       RpcTransport 实现 (tell/ask via Stream)
│   ├── RedisRpcConsumer        Master 消费 rpc:master:{serverId}
│   └── RedisRpcResponseConsumer  Pod 消费 ask 响应
│
├── dynamodb/                   DynamoDB 默认实现
│   ├── DynamoDbClientFactory   客户端工厂
│   ├── DynamoDbItem            单表模型 (pk/sk/data/ttl/version)
│   ├── DynamoDbKeyStrategy     键映射策略接口
│   ├── DynamoDbKeyStrategies   内置策略 (player/guild/guildMember/serverData)
│   ├── DynamoDbValueSerializer 值序列化接口 + Jackson 实现
│   ├── DynamoDbPersistenceProvider  PersistenceProvider 实现
│   ├── DynamoDbQueryHelper     查询辅助 (pk/prefix/range)
│   ├── DynamoDbBatchLoader     批量加载（Actor 启动恢复）
│   └── DynamoDbTransactionHelper  事务写入
│
├── base/                       Actor 基类
│   ├── StashableActor          异步加载 → stash → unstash → active
│   ├── TickableActor           带定时 tick 的 Actor
│   └── SupervisedActor         Supervision 策略工具
│
├── lifecycle/                  生命周期管理
│   ├── FlushableActor          支持 flush 的 Actor 标记
│   └── GracefulShutdown        优雅停机（并行 flush → 超时 → terminate）
│
├── view/                       视野推送基础
│   ├── ViewEvent               record(objectId, snapshot, changeType, gridId, areaId)
│   ├── ChangeType              enum: ADDED, CHANGED, REMOVED
│   ├── GridId                  record(x, y)
│   ├── ViewThrottler           节流器（100ms 合并，超阈值丢弃旧事件）
│   └── GridSubscription        Grid→Player 反转索引
│
├── scene/                      场景 Actor 体系
│   ├── SceneMessage            Init / Shutdown
│   ├── MapSceneActor           场景根，spawn 所有子 Actor
│   │
│   ├── aoi/                    AOI 系统（核心）
│   │   ├── WorldMapConfig      地图配置 (1000x1000, 75x75 区域, 169 areas)
│   │   ├── ViewPort            视口矩形
│   │   ├── AoiCalculator       视口→区域映射, diff 计算, Bresenham 路径
│   │   ├── AoiDiff             视口移动变更 (added/removed/current)
│   │   ├── AoiManager          玩家 AOI 状态管理
│   │   ├── AreaViewNotifier    区域级 Grid→Player 通知分发
│   │   ├── SpatialIndex        空间索引 (Grid→objectId)
│   │   └── MarchPathCalculator 行军路径计算
│   │
│   ├── area/                   区域 Actor
│   │   ├── AreaActor           Stash 加载 + 500ms tick + 1s 持久化 + 视野推送
│   │   ├── AreaMessage         sealed interface (14 种消息)
│   │   ├── AreaState           内存状态 (objects/dirty/marchLines)
│   │   ├── MapObject           地图对象接口
│   │   └── AreaBehavior        业务扩展点
│   │
│   ├── march/                  行军 Actor
│   │   ├── MarchSupervisor     创建/管理 MarchActor
│   │   ├── MarchActor          定时器驱动（注册路线→到达→战斗→返程→停止）
│   │   ├── MarchMessage        sealed interface
│   │   ├── MarchState          行军状态
│   │   └── MarchBehavior       业务扩展点
│   │
│   ├── rally/                  集结 Actor
│   │   ├── RallySupervisor     创建/管理 RallyActor
│   │   ├── RallyActor          多阶段状态机 (WAITING→MARCHING→BATTLING→RETURNING)
│   │   ├── RallyMessage        sealed interface
│   │   ├── RallyState          集结状态
│   │   └── RallyBehavior       业务扩展点
│   │
│   └── event/                  事件 Actor
│       ├── EventSupervisor     创建 WorldBoss/GuildWar
│       ├── WorldBossActor      Boss (血量/阶段/伤害排行)
│       ├── WorldBossMessage    sealed interface
│       ├── GuildWarActor       公会战 (据点/计分/阶段)
│       └── GuildWarMessage     sealed interface
│
├── player/                     玩家 Actor
│   ├── PlayerActor             master/slave 统一，集成 AoiManager
│   ├── PlayerMessage           sealed interface (10 种消息)
│   ├── PlayerSession           网络层抽象
│   ├── PlayerMessageHandler    业务消息处理接口
│   └── PlayerActorFactory      工厂接口
│
└── guild/                      公会 Actor
    ├── GuildActor              串行化操作 + 持久化
    ├── GuildMessage            sealed interface (12 种消息)
    ├── GuildState              公会状态
    └── GuildBehavior           业务扩展点
```

## 核心设计

### 1. 路由决策（ActorRouter）

所有 Actor 间通信统一走 `ActorRouter`，它根据目标地址自动决定本地调用还是远程 RPC：

```java
private boolean isLocal(ActorAddress target) {
    if (target.serverId() != currentServerId) return false;  // 跨服一定远程
    return switch (target.type()) {
        case PLAYER -> true;  // PlayerActor 总在本地
        case GUILD, AREA, MARCH_SUPERVISOR, ... -> isMaster;  // 只有 master 有这些 Actor
    };
}
```

这意味着跨服战和 master/slave 通信用同一套代码，只是 `serverId` 不同。

### 2. AOI 视野系统

地图 1000×1000 格子，划分为 169 个区域（每个 75×75）。

**视口订阅流程**：
```
玩家打开地图 → AoiManager.openMap(视口坐标)
  → AoiCalculator.calcSubscribedAreas(视口)
  → 返回 Map<areaId, Set<GridId>>
  → Master: 向每个 AreaActor 发送 Subscribe(playerRef, grids)
  → Slave: 订阅 Redis Stream scene:change:{serverId}:{areaId}
```

**视口移动**：
```
玩家拖动地图 → AoiManager.moveView(新视口)
  → AoiCalculator.calcViewDiff(旧视口, 新视口)
  → 返回 AoiDiff(added, removed, current)
  → 只对变更的区域发送 Subscribe/Unsubscribe/UpdateSubscription
```

**对象变更通知**：
```
AreaActor tick → 对象状态变更 → ViewEvent
  → AreaViewNotifier.notifyBatch(events)  // 按 gridId 分发给本地订阅者
  → RedisStreamPublisher.publish(...)     // 推送给 slave Pod 上的玩家
```

### 3. 行军系统（无 Tick 设计）

MarchActor 不使用定时 tick，而是：
1. 出发时计算完整路径 + 到达时间
2. 设置单次定时器（`ArrivalTimeout`）
3. 向沿途 AreaActor 注册路线（客户端自己插值显示）
4. 到达时触发战斗/采集
5. 结束后注销路线，创建返程 MarchActor，自我停止

支持中断操作：Recall（召回）、Intercept（拦截）、SpeedUp（加速）。

### 4. 集结系统（状态机）

RallyActor 使用 Pekko Typed 的 Behavior 切换实现多阶段状态机：

```
WAITING (接收 Join/Leave, 超时或满员触发出发)
  → MARCHING (设置到达定时器)
    → BATTLING (等待战斗结果)
      → RETURNING (为每个成员创建返程 MarchActor)
        → DISMISSED (通知所有成员, Behaviors.stopped())
```

### 5. 持久化策略

| Actor | 持久化目标 | 策略 |
|-------|-----------|------|
| PlayerActor | DynamoDB | 已有逻辑不变 |
| GuildActor | DynamoDB + Redis 缓存 | 写同步 DDB，更新 Redis 供读 |
| AreaActor | Redis | 1s 异步批量，关键操作同步 |
| MarchActor | 不持久化 | 短生命周期，丢了可接受 |
| RallyActor | Redis | 等待阶段需持久化 |
| WorldBossActor | Redis | 定期快照 |

### 6. 优雅停机

```
SIGTERM → preStop hook (sleep 5s)
  → 停止接收新连接
  → GracefulShutdown.shutdown(timeout=10s)
    → 并行通知所有 FlushableActor flush
    → 等待 ack（超时强制）
    → ActorSystem.terminate()
```

### 7. DynamoDB 单表设计

使用 `pk + sk + data(JSON)` 的单表模式：

```
pk                  | sk              | data
--------------------|-----------------|------------------
PLAYER#10001        | PROFILE         | {"name":"...", ...}
GUILD#5001          | INFO            | {"name":"...", "level":5, ...}
GUILD#5001          | MEMBER#10001    | {"rank":1, "joinTime":...}
SERVER#1            | WORLD_BOSS#1    | {"hp":50000, "phase":2, ...}
```

通过 `DynamoDbKeyStrategy` 接口自定义键映射规则。

## 扩展点

框架通过 Behavior 接口留出业务扩展点，game-server 实现具体逻辑：

| 接口 | 需要实现的方法 |
|------|--------------|
| `AreaBehavior` | onTick, onAttack, generateInitialObjects |
| `MarchBehavior` | calcArrivalTime, onBattleResult, toLineInfo, gridToAreaMapper |
| `RallyBehavior` | canJoin, onDepart, onArrival, calcMarchDuration |
| `GuildBehavior` | canJoin, canKick, onDonate, maxMembers |
| `WorldBossActor.WorldBossBehavior` | getMaxHp, calcDamage, calcPhase, onBossDead |
| `GuildWarActor.GuildWarBehavior` | canCapture, onCapture, getPointScore, onEnd |
| `PlayerMessageHandler` | handleRequest, buildViewUpdate, deserializeViewEvent |
| `PlayerSession` | send, close, isConnected |

## 快速开始

```java
// 1. 创建框架实例
var redisTemplate = ...; // Spring 注入
var serializer = new JacksonRpcSerializer();
var rpcTransport = new RedisRpcTransport(redisTemplate, serializer);
var framework = ActorFramework.create(rpcTransport);

// 2. 创建 DynamoDB 持久化
var dynamoFactory = new DynamoDbClientFactory("ap-southeast-1");
var guildPersistence = new DynamoDbPersistenceProvider<>(
    dynamoFactory.getEnhancedClient(), "game-data",
    DynamoDbKeyStrategies.guild(),
    DynamoDbValueSerializer.jackson(objectMapper, GuildState.class)
);

// 3. 启动场景（仅 Master）
if (framework.getRoleProvider().isMaster()) {
    var sceneRef = framework.getSystem().systemActorOf(
        MapSceneActor.create(framework.getRegistry(), framework.getRouter(),
            redisCacheProvider, redisStreamPublisher, serializer,
            framework.getRoleProvider().getServerId(),
            myAreaBehavior, myMarchBehavior, myRallyBehavior),
        "map-scene"
    );
    sceneRef.tell(new SceneMessage.Init(169));

    // 启动 RPC 消费者
    var rpcConsumer = new RedisRpcConsumer(redisTemplate, serializer,
        framework.getRpcMessageHandler(), framework.getRoleProvider().getServerId());
    rpcConsumer.start();
}

// 4. 玩家连接时创建 PlayerActor
var aoiCalc = new AoiCalculator(WorldMapConfig.defaultConfig());
var playerRef = framework.getSystem().systemActorOf(
    PlayerActor.create(playerId, session, framework.getRoleProvider().isMaster(),
        framework.getRouter(), redisStreamConsumer,
        framework.getRoleProvider().getServerId(), myMessageHandler, aoiCalc),
    "player-" + playerId
);
framework.getRegistry().register(ActorType.PLAYER, playerId, playerRef);
```

## 部署模型

```yaml
# Master: 固定 1 个 Pod
game-server-master:
  replicas: 1
  strategy: Recreate
  env: SERVER_ROLE=master, SERVER_ID=1
  resources: cpu 2-4, memory 4-8Gi

# Slave: HPA 管理
game-server-slave:
  replicas: 2-5 (HPA by CPU 70%)
  env: SERVER_ROLE=slave, SERVER_ID=1
  resources: cpu 1-2, memory 2-4Gi
```

Master 故障恢复：K8s Recreate 重建，~30-40s 写不可用，读不受影响。

## Redis Key 规划

```
server:{serverId}:scene:area:{areaId}:obj:{objectId}  — 场景对象缓存
scene:change:{serverId}:{areaId}                       — 视野推送 Stream
rpc:master:{serverId}                                  — Master RPC 入口 Stream
rpc:pod:{podId}                                        — Pod 级 ask 响应 Stream
```

## 邮箱配置

```hocon
# AreaActor / GuildActor — 关键消息不可丢
critical-mailbox { capacity = 5000, push-timeout = 3s }

# MarchActor / RallyActor — 消息量小
task-mailbox { capacity = 500, push-timeout = 3s }

# PlayerActor — 大容量，视野节流在应用层
player-mailbox { capacity = 3000, push-timeout = 1s }
```

## 构建

```bash
mvn compile    # 编译
mvn package    # 打包
mvn install    # 安装到本地仓库，供 game-server 引用
```

## 依赖引入（game-server pom.xml）

```xml
<dependency>
    <groupId>io.nexus</groupId>
    <artifactId>actor-framework</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
