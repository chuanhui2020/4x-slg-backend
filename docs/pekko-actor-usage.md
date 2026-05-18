# Pekko Actor 使用总结

本项目使用 Apache Pekko (v1.1.3) 的 Typed Actor 模块作为单机 Actor 调度引擎，负责游戏实体的并发隔离与消息驱动。项目未使用 Pekko Cluster/Sharding/Remote 等分布式功能，跨节点通信由自建的 gRPC + Redis Stream 层承担。

## 依赖

```xml
<groupId>org.apache.pekko</groupId>
<artifactId>pekko-actor-typed_2.13</artifactId>
<version>1.1.3</version>
```

## 使用的 Pekko 功能

### 1. ActorSystem

`ActorFramework.create()` 中创建唯一的 `ActorSystem<Void>` 实例，作为整个进程的 Actor 运行时容器：

```java
Behavior<Void> rootBehavior = Behaviors.empty();
ActorSystem<Void> system = ActorSystem.create(rootBehavior, "actor-framework");
```

用途：
- 提供 Scheduler（用于 AskPattern 超时）
- 提供 Actor spawn 的根上下文
- 优雅关闭时调用 `system.terminate()`

### 2. Typed Actor（AbstractBehavior）

所有游戏实体 Actor 继承 `AbstractBehavior<T>`，通过 `Behaviors.setup()` 工厂方法创建：

```java
public class PlayerActor extends AbstractBehavior<PlayerMessage> {
    public static Behavior<PlayerMessage> create(...) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new PlayerActor(ctx, timers, ...)));
    }

    @Override
    public Receive<PlayerMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(PlayerMessage.SessionMsg.class, this::onSessionMessage)
                .onMessage(PlayerMessage.Stop.class, msg -> Behaviors.stopped())
                .build();
    }
}
```

项目中的 Actor 实体：

| Actor | 消息类型 | 职责 |
|-------|---------|------|
| `PlayerActor` | `PlayerMessage` | 玩家会话、AOI 视野管理、视图事件聚合 |
| `AreaActor` | `AreaMessage` | 地图区域状态、格子数据、订阅者管理 |
| `MapSceneActor` | `SceneMessage` | 场景总管，spawn 并管理子 Actor |
| `MarchActor` | `MarchMessage` | 单次行军逻辑 |
| `MarchSupervisor` | `MarchMessage` | 行军生命周期管理 |
| `RallyActor` | `RallyMessage` | 单次集结逻辑 |
| `RallySupervisor` | `RallyMessage` | 集结生命周期管理 |
| `GuildActor` | — | 公会状态 |
| `WorldBossActor` | — | 世界 Boss 事件 |
| `GuildWarActor` | — | 公会战事件 |
| `EventSupervisor` | `EventCommand` | 世界事件调度 |

### 3. Actor 层级与 Spawn

`MapSceneActor` 作为父 Actor，在初始化时 spawn 子 Actor 并注册到 `ActorRegistry`：

```java
private Behavior<SceneMessage> onInit(SceneMessage.Init msg) {
    // Spawn AreaActors
    for (int i = 0; i < areaCount; i++) {
        ActorRef<AreaMessage> areaRef = getContext().spawn(areaBeh, "area-" + i);
        registry.register(ActorType.AREA, i, areaRef);
    }
    // Spawn MarchSupervisor
    ActorRef<MarchMessage> marchRef = getContext().spawn(marchBeh, "march-supervisor");
    registry.register(ActorType.MARCH_SUPERVISOR, 0, marchRef);
    // ...
}
```

层级结构：
```
ActorSystem (root)
└── MapSceneActor
    ├── AreaActor-0 .. AreaActor-N
    ├── MarchSupervisor
    │   └── MarchActor (动态 spawn)
    ├── RallySupervisor
    │   └── RallyActor (动态 spawn)
    └── EventSupervisor
        ├── WorldBossActor
        └── GuildWarActor
```

### 4. TimerScheduler

通过 `Behaviors.withTimers()` 获取 `TimerScheduler`，用于周期性和延迟任务。

**TickableActor 基**封装了通用定时逻辑：

```java
public abstract class TickableActor<T> extends AbstractBehavior<T> {
    protected void startTick() {
        timers.startTimerWithFixedDelay("tick", buildTickMessage(), tickInterval());
    }
    protected void startTimer(String key, T message, Duration delay) {
        timers.startSingleTimer(key, message, delay);
    }
}
```

**PlayerActor** 中的使用：
- `startTimerWithFixedDelay("flush", ...)` — 定期将聚合的视野事件 flush 给客户端
- 打开地图时启动，关闭地图时取消

### 5. Ask Pattern

`AskPattern.ask()` 用于需要 request-response 语义的本地 Actor 调用：

**LocalActorInvoker** — 路由层对本地 Actor 的 ask 调用：
```java
public <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout) {
    ActorRef<Object> ref = registry.resolveOrThrow(target.type(), target.id());
    return AskPattern.<Object, T>ask(ref, replyTo -> new AskWrapper<>(message, replyTo),
            timeout, system.scheduler()).toCompletableFuture();
}
```

**GracefulShutdown** — 关闭时 ask 每个 Actor 执行 flush：
```java
private <T> CompletableFuture<FlushableActor.FlushComplete> askFlush(FlushEntry<T> entry, Duration timeout) {
    return AskPattern.ask(entry.actor(),
            replyTo -> entry.flushMessageFactory().apply(replyTo),
            timeout, system.scheduler()).toCompletableFuture();
}
```

### 6. Supervisor Strategy

`SupervisedActor` 工具类封装了四种监督策略：

```java
public final class SupervisedActor {
    // 异常后立即重启
    public static <T> Behavior<T> restart(Behavior<T> behavior);

    // 异常后指数退避重启
    public static <T> Behavior<T> restartWithBackoff(Behavior<T> behavior,
            Duration minBackoff, Duration maxBackoff, double randomFactor);

    // 异常后停止 Actor
    public static <T> Behavior<T> stop(Behavior<T> behavior);

    // 忽略异常继续运行
    public static <T> Behavior<T> resume(Behavior<T> behavior);
}
```

使用方式：用 `SupervisedActor.restart(behavior)` 包装原始 Behavior 即可。

### 7. Stash Buffer

`StashableActor` 基类实现了异步加载期间的消息暂存：

```java
public abstract class StashableActor<T> extends AbstractBehavior<T> {
    private final StashBuffer<T> stash;

    // 加载中：暂存所有消息
    protected Receive<T> loading() {
        return newReceiveBuilder()
                .onMessageEquals(buildLoadCompleteMessage(), () -> stash.unstashAll(active()))
                .onAnyMessage(msg -> { stash.stash(msg); return Behaviors.same(); })
                .build();
    }

    // 子类实现异步加载逻辑
    protected abstract CompletableFuture<Void> loadAsync();
    // 加载完成后切换到正常处理
    protected abstract Receive<T> active();
}
```

典型场景：Actor 启动时需要从 DynamoDB 加载状态，加载期间暂存到达的消息，加载完成后 unstash 并切换到 active 状态。

### 8. Behaviors.stopped()

Actor 通过返回 `Behaviors.stopped()` 自行终止：

```java
.onMessage(PlayerMessage.Stop.class, msg -> Behaviors.stopped())
```

`MapSceneActor` 在 shutdown 时先通知 ack，再返回 stopped。

## 未使用的 Pekko 功能

| 功能 | 替代方案 |
|------|---------|
| Pekko Cluster / Sharding | 自建 Master-Slave 架构 + Redis 位置注册 |
| Pekko Remote | gRPC（Slave→Master 命令）+ Redis Stream（消息投递） |
| Pekko Persistence (Event Sourcing) | DynamoDB 持久化 |
| Pekko Streams | Redis Stream + 自建 StreamPublisher/Consumer |
| Pekko Routers | 自建 ActorRouter（Local/Remote 路由判断） |
| Pekko Distributed Data | Redis 作为共享状态存储 |

## 设计决策

项目选择轻量使用 Pekko 的原因：

1. **只取并发模型** — Actor 的消息隔离天然适合游戏实体（玩家、区域、行军），无需手动加锁
2. **自控分布式层** — 游戏服务器的拓扑（固定 Master + 可扩展 Slave）比 Pekko Cluster 的对等模型更简单可控
3. **避免重量级依赖** — 不引入 Pekko Cluster 的复杂性（split-brain、gossip、shard rebalance）
4. **灵活的跨节点通信** — gRPC 用于同步命令，Redis Stream 用于异步事件广播，各取所长
