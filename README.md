# Nexus Actor Framework

[![Java](https://img.shields.io/badge/Java-25-orange)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.0.0-green)](https://spring.io/projects/spring-boot)
[![Pekko](https://img.shields.io/badge/Apache%20Pekko-1.1.3-blue)](https://pekko.apache.org/)
[![gRPC](https://img.shields.io/badge/gRPC-1.68.0-blueviolet)](https://grpc.io/)
[![License](https://img.shields.io/badge/License-Private-red)]()

基于 Apache Pekko Typed 的游戏服务器 Actor 框架，专为 4X-SLG 类型游戏设计。支持 Master/Slave 分离部署、gRPC 强交互写请求、Redis Stream 异步事件、AOI 视野推送，单服承载 2w 在线玩家。

---

## 架构总览

<p align="center">
  <img src="https://mermaid.ink/img/Z3JhcGggVEIKICAgIHN1YmdyYXBoIENsaWVudHMKICAgICAgICBDMVtDbGllbnQgMV0KICAgICAgICBDMltDbGllbnQgMl0KICAgICAgICBDM1tDbGllbnQgTl0KICAgIGVuZAoKICAgIHN1YmdyYXBoIFNsYXZlX1BvZF9BW1NsYXZlIFBvZCBBXQogICAgICAgIFNBX1BBW1BsYXllckFjdG9yIDEuLk5dCiAgICAgICAgU0FfQU9JW0FPSSBGaWx0ZXJdCiAgICBlbmQKCiAgICBzdWJncmFwaCBTbGF2ZV9Qb2RfQltTbGF2ZSBQb2QgQl0KICAgICAgICBTQl9QQVtQbGF5ZXJBY3RvciAxLi5OXQogICAgICAgIFNCX0FPSVtBT0kgRmlsdGVyXQogICAgZW5kCgogICAgc3ViZ3JhcGggTWFzdGVyX1BvZFtNYXN0ZXIgUG9kXQogICAgICAgIGRpcmVjdGlvbiBUQgogICAgICAgIE1TW01hcFNjZW5lQWN0b3JdCiAgICAgICAgQUFbQXJlYUFjdG9yIDF+MTk2XQogICAgICAgIE1BUkNIW01hcmNoU3VwZXJ2aXNvcl0KICAgICAgICBSQUxMWVtSYWxseVN1cGVydmlzb3JdCiAgICAgICAgRVZFTlRbRXZlbnRTdXBlcnZpc29yXQogICAgICAgIEdVSUxEW0d1aWxkQWN0b3JdCiAgICAgICAgTUFbTWFyY2hBY3RvciAqXQogICAgICAgIFJBW1JhbGx5QWN0b3IgKl0KICAgICAgICBXQltXb3JsZEJvc3NBY3Rvcl0KICAgICAgICBHV1tHdWlsZFdhckFjdG9yXQoKICAgICAgICBNUyAtLT4gQUEKICAgICAgICBNUyAtLT4gTUFSQ0gKICAgICAgICBNUyAtLT4gUkFMTFkKICAgICAgICBNUyAtLT4gRVZFTlQKICAgICAgICBNQVJDSCAtLT4gTUEKICAgICAgICBSQUxMWSAtLT4gUkEKICAgICAgICBFVkVOVCAtLT4gV0IKICAgICAgICBFVkVOVCAtLT4gR1cKICAgIGVuZAoKICAgIHN1YmdyYXBoIFN0b3JhZ2UKICAgICAgICBSZWRpc1soUmVkaXMpXQogICAgICAgIEREQlsoRHluYW1vREIpXQogICAgZW5kCgogICAgQzEgLS0+IFNBX1BBCiAgICBDMiAtLT4gU0FfUEEKICAgIEMzIC0tPiBTQl9QQQoKICAgIFNBX1BBIC0tPnxnUlBDIFdyaXRlfCBNYXN0ZXJfUG9kCiAgICBTQl9QQSAtLT58Z1JQQyBXcml0ZXwgTWFzdGVyX1BvZAoKICAgIE1hc3Rlcl9Qb2QgLS0+fFB1Yi9TdWIgQU9JfCBSZWRpcwogICAgUmVkaXMgLS0+fEFPSSBFdmVudHN8IFNBX0FPSQogICAgUmVkaXMgLS0+fEFPSSBFdmVudHN8IFNCX0FPSQogICAgU0FfQU9JIC0tPiBTQV9QQQogICAgU0JfQU9JIC0tPiBTQl9QQQoKICAgIE1hc3Rlcl9Qb2QgLS0+fFN0cmVhbSBBc3luY3wgUmVkaXMKICAgIFJlZGlzIC0tPnxSZWxpYWJsZSBFdmVudHN8IFNBX1BBCiAgICBSZWRpcyAtLT58UmVsaWFibGUgRXZlbnRzfCBTQl9QQQoKICAgIE1hc3Rlcl9Qb2QgLS0+IEREQgogICAgTWFzdGVyX1BvZCAtLT4gUmVkaXMK" alt="Architecture Overview" />
</p>

---

## 通信架构

<p align="center">
  <img src="https://mermaid.ink/img/Zmxvd2NoYXJ0IExSCiAgICBzdWJncmFwaCBTbGF2ZQogICAgICAgIFBsYXllcltQbGF5ZXJBY3Rvcl0KICAgIGVuZAoKICAgIHN1YmdyYXBoIFRyYW5zcG9ydAogICAgICAgIGdSUEN7e2dSUEN9fQogICAgICAgIFB1YlN1Ynt7UmVkaXMgUHViL1N1Yn19CiAgICAgICAgU3RyZWFte3tSZWRpcyBTdHJlYW19fQogICAgZW5kCgogICAgc3ViZ3JhcGggTWFzdGVyCiAgICAgICAgQ01EW0NvbW1hbmREaXNwYXRjaGVyXQogICAgICAgIEFyZWFbQXJlYUFjdG9yXQogICAgICAgIE1hcmNoW01hcmNoU3VwZXJ2aXNvcl0KICAgICAgICBHdWlsZFtHdWlsZEFjdG9yXQogICAgZW5kCgogICAgUGxheWVyIC0tPnxXcml0ZSBSZXF1ZXN0fCBnUlBDCiAgICBnUlBDIC0tPnxDb21tYW5kUmVxdWVzdHwgQ01ECiAgICBDTUQgLS0+fEFzayBQYXR0ZXJufCBNYXJjaAogICAgQ01EIC0tPnxBc2sgUGF0dGVybnwgR3VpbGQKICAgIENNRCAtLT58Q29tbWFuZFJlc3BvbnNlfCBnUlBDCiAgICBnUlBDIC0tPnxSZXNwb25zZXwgUGxheWVyCgogICAgQXJlYSAtLT58QU9JIGJhdGNoIDEwMG1zfCBQdWJTdWIKICAgIFB1YlN1YiAtLT58U2NlbmUgQ2hhbmdlc3wgUGxheWVyCgogICAgTWFzdGVyIC0tPnxBc3luYyBSZXN1bHRzfCBTdHJlYW0KICAgIFN0cmVhbSAtLT58UmVsaWFibGUgKyBYQUNLfCBQbGF5ZXIK" alt="Communication Architecture" />
</p>

| 方向 | 方式 | 用途 |
|------|------|------|
| Slave → Master | gRPC | 写请求（CREATE_MARCH, JOIN_GUILD, ATTACK...） |
| Master → Slave (即时) | gRPC Response | 请求内直接返回结果 |
| Master → Slave (异步) | Redis Stream | 行军到达、战斗结算等异步结果 |
| AOI 场景变化 | Redis Pub/Sub | batch 广播，fire-and-forget |

---

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Java | 25 | Virtual Threads + Sealed Interface + Record |
| Spring Boot | 4.0.0 | DI + Auto Configuration + Actuator |
| Apache Pekko Typed | 1.1.3 | Actor 模型核心 |
| gRPC | 1.68.0 | Slave → Master 强交互写请求 |
| Redis (Lettuce) | Spring Boot 管理 | Pub/Sub AOI + Stream RPC + 缓存 + Lease |
| AWS DynamoDB | SDK 2.31.1 | 玩家/公会持久化 |
| Micrometer + Prometheus | Spring Boot 管理 | 可观测性指标 |

---

## 核心特性

### AOI 视野系统

地图 1000x1000 格子，划分为 196 个区域（14x14 布局，每区域 75x75）。

```
AreaActor Tick (100ms)
    │
    ▼
聚合变化（同对象多次变化只保留最后一次）
    │
    ▼
Redis Pub/Sub 按 Area 发布 batch
    │
    ▼
Slave 订阅相关 Area Channel
    │
    ▼
Grid 级精确过滤（只推送视野内事件）
    │
    ▼
推送给客户端
```

- 视口订阅：玩家 openMap 时按视口范围订阅 Area
- 移动 diff：moveView 只处理增量变化（新增/移除的 Area 和 Grid）
- 背压控制：ViewThrottler 限制 pending 事件数量

### 行军系统（无 Tick）

MarchActor 不使用定时 tick，出发时计算完整路径和到达时间，设置单次定时器。支持召回、拦截、加速等中断操作。路径经过的 Area 会收到 MarchLineEnter/Leave 通知。

### 集结系统（状态机）

```
WAITING ──→ MARCHING ──→ BATTLING ──→ RETURNING ──→ DISMISSED
   │                                                    ▲
   └────────────────── Dismiss ─────────────────────────┘
```

RallyActor 使用 Pekko Typed Behavior 切换实现多阶段状态机，每个阶段只处理该阶段合法的消息。

### Master Lease 高可用

<p align="center">
  <img src="https://mermaid.ink/img/c2VxdWVuY2VEaWFncmFtCiAgICBwYXJ0aWNpcGFudCBBY3RpdmUgYXMgQWN0aXZlIE1hc3RlcgogICAgcGFydGljaXBhbnQgUmVkaXMgYXMgUmVkaXMgTGVhc2UKICAgIHBhcnRpY2lwYW50IFN0YW5kYnkgYXMgU3RhbmRieSBNYXN0ZXIKCiAgICBBY3RpdmUtPj5SZWRpczogU0VUIG1hc3Rlcl9sZWFzZSBQWCAzMDAwMAogICAgbG9vcCBFdmVyeSAxMHMKICAgICAgICBBY3RpdmUtPj5SZWRpczogUEVYUElSRSByZW5ldwogICAgICAgIFJlZGlzLS0+PkFjdGl2ZTogT0sKICAgIGVuZAogICAgTm90ZSBvdmVyIEFjdGl2ZTogUHJvY2VzcyBjcmFzaAogICAgU3RhbmRieS0+PlJlZGlzOiBHRVQgbWFzdGVyX2xlYXNlCiAgICBOb3RlIG92ZXIgU3RhbmRieTogVFRMIGV4cGlyZWQKICAgIFN0YW5kYnktPj5SZWRpczogU0VUICsgSU5DUiBlcG9jaAogICAgUmVkaXMtLT4+U3RhbmRieTogZXBvY2g9NDMKICAgIE5vdGUgb3ZlciBTdGFuZGJ5OiBCZWNvbWUgQWN0aXZlCg==" alt="Master Lease Failover" />
</p>

---

## 持久化策略

| Actor | 存储 | 策略 |
|-------|------|------|
| PlayerActor | DynamoDB | 已有逻辑 |
| GuildActor | DynamoDB + Redis | 写同步 DDB，Redis 供读 |
| AreaActor | Redis | 1s 异步批量，失败 reMarkDirty |
| MarchActor | Redis | MarchStateStore 持久化活跃行军 |
| RallyActor / WorldBoss | Redis | 定期快照 |

---

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

---

## 部署模型

<p align="center">
  <img src="https://mermaid.ink/img/Z3JhcGggVEIKICAgIHN1YmdyYXBoIEs4c1tLdWJlcm5ldGVzIENsdXN0ZXJdCiAgICAgICAgc3ViZ3JhcGggU1NbU3RhdGVmdWxTZXRdCiAgICAgICAgICAgIE1bTWFzdGVyIFBvZApTRVJWRVJfUk9MRT1tYXN0ZXIKQ1BVIDItNCAvIE1lbSA0LThHaV0KICAgICAgICBlbmQKICAgICAgICBzdWJncmFwaCBEZXBsb3lbRGVwbG95bWVudCAtIEhQQV0KICAgICAgICAgICAgUzFbU2xhdmUgUG9kIDEKU0VSVkVSX1JPTEU9c2xhdmVdCiAgICAgICAgICAgIFMyW1NsYXZlIFBvZCAyClNFUlZFUl9ST0xFPXNsYXZlXQogICAgICAgICAgICBTM1tTbGF2ZSBQb2QgTgpTRVJWRVJfUk9MRT1zbGF2ZV0KICAgICAgICBlbmQKICAgICAgICBTVkNbZ2FtZS1tYXN0ZXIgU2VydmljZSA6OTA5MF0KICAgIGVuZAoKICAgIFMxIC0tPiBTVkMKICAgIFMyIC0tPiBTVkMKICAgIFMzIC0tPiBTVkMKICAgIFNWQyAtLT4gTQo=" alt="Deployment Model" />
</p>

| 角色 | 副本数 | 环境变量 | 资源 | 部署策略 |
|------|--------|----------|------|----------|
| Master | 1 | `SERVER_ROLE=master` `SERVER_ID=1` | CPU 2-4, Mem 4-8Gi | StatefulSet / Recreate |
| Slave | 2-5 | `SERVER_ROLE=slave` `SERVER_ID=1` | CPU 1-2, Mem 2-4Gi | Deployment / HPA by CPU 70% |

---

## 快速开始

```java
// 创建框架实例
var rpcTransport = new RedisRpcTransport(redisTemplate, serializer,
        serverId, podId, 4096, 30);
var framework = ActorFramework.create(rpcTransport, metrics);

// Master: 启动 gRPC Server
if (framework.getRoleProvider().isMaster()) {
    framework.startGrpcServer();

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
        router, streamConsumer, serverId, handler, aoiCalc, 200, 100),
    "player-" + playerId);
framework.getRegistry().register(ActorType.PLAYER, playerId, playerRef);
```

---

## 构建

```bash
mvn compile    # 编译（含 protobuf 代码生成）
mvn package    # 打包
mvn install    # 安装到本地仓库
```

## 配置

核心配置项在 `application.yml` 中，支持环境变量覆盖：

```yaml
game:
  server-id: ${SERVER_ID:1}
  pod-id: ${POD_ID:local}
  server-role: ${SERVER_ROLE:master}
  grpc:
    deadline-ms: 2000
  dispatch:
    max-concurrent: 1024
  lease:
    ttl-seconds: 30
    heartbeat-interval-ms: 10000
```

---

## 可观测性

内置 Micrometer + Prometheus 指标：

| 指标 | 说明 |
|------|------|
| gRPC 调用延迟 | CommandDispatcher 计时 |
| 并发 dispatch 数 | 实时在途请求数 |
| dispatch 错误率 | 失败/超时计数 |
| Lease 续约延迟 | Master 心跳计时 |
| Lease epoch | 当前主备代次 |

访问 `/actuator/prometheus` 获取指标数据。

---

## 项目结构

```
src/main/java/io/nexus/
├── ActorFramework.java          # 框架入口
├── address/                     # ActorAddress + ActorType
├── base/                        # StashableActor, TickableActor
├── dynamodb/                    # DynamoDB 持久化
├── grpc/                        # gRPC Client/Server/Dispatcher
├── guild/                       # 公会 Actor
├── lifecycle/                   # GracefulShutdown, FlushableActor
├── metrics/                     # GameMetrics, HealthIndicators
├── persistence/                 # 持久化抽象接口
├── player/                      # 玩家 Actor
├── redis/                       # Redis 通信层
├── registry/                    # Actor 注册表
├── role/                        # ServerRoleProvider, MasterLeaseManager
├── router/                      # ActorRouter 路由层
├── rpc/                         # RPC 消息处理
├── scene/                       # 场景系统
│   ├── aoi/                     # AOI 视野计算
│   ├── area/                    # 区域 Actor
│   ├── event/                   # 世界事件 (WorldBoss, GuildWar)
│   ├── march/                   # 行军系统
│   └── rally/                   # 集结系统
├── serialize/                   # 序列化
└── view/                        # 视野事件
```
