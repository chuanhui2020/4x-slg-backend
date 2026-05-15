package io.nexus.actor.scene.area;

import io.nexus.actor.persistence.CacheProvider;
import io.nexus.actor.persistence.StreamPublisher;
import io.nexus.actor.player.PlayerMessage;
import io.nexus.actor.router.ActorRouter;
import io.nexus.actor.scene.aoi.AreaViewNotifier;
import io.nexus.actor.scene.aoi.SpatialIndex;
import io.nexus.actor.serialize.RpcSerializer;
import io.nexus.actor.view.*;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class AreaActor extends AbstractBehavior<AreaMessage> {

    private static final Duration TICK_INTERVAL = Duration.ofMillis(500);
    private static final Duration PERSIST_INTERVAL = Duration.ofMillis(1000);

    private final StashBuffer<AreaMessage> stash;
    private final TimerScheduler<AreaMessage> timers;
    private final int areaId;
    private final int serverId;
    private final AreaState state;
    private final AreaBehavior behavior;
    private final CacheProvider cache;
    private final StreamPublisher streamPublisher;
    private final ActorRouter router;
    private final RpcSerializer serializer;
    private final AreaViewNotifier viewNotifier;
    private final SpatialIndex spatialIndex;

    public static Behavior<AreaMessage> create(int areaId, int serverId, AreaBehavior behavior,
                                                CacheProvider cache, StreamPublisher streamPublisher,
                                                ActorRouter router, RpcSerializer serializer) {
        return Behaviors.withStash(5000, stash ->
                Behaviors.withTimers(timers ->
                        Behaviors.setup(ctx -> new AreaActor(ctx, stash, timers, areaId, serverId,
                                behavior, cache, streamPublisher, router, serializer))));
    }

    private AreaActor(ActorContext<AreaMessage> context, StashBuffer<AreaMessage> stash,
                       TimerScheduler<AreaMessage> timers, int areaId, int serverId,
                       AreaBehavior behavior, CacheProvider cache, StreamPublisher streamPublisher,
                       ActorRouter router, RpcSerializer serializer) {
        super(context);
        this.stash = stash;
        this.timers = timers;
        this.areaId = areaId;
        this.serverId = serverId;
        this.state = new AreaState(areaId);
        this.behavior = behavior;
        this.cache = cache;
        this.streamPublisher = streamPublisher;
        this.router = router;
        this.serializer = serializer;
        this.viewNotifier = new AreaViewNotifier(areaId);
        this.spatialIndex = new SpatialIndex();
        startAsyncLoad();
    }

    @Override
    public Receive<AreaMessage> createReceive() {
        return loading();
    }

    private Receive<AreaMessage> loading() {
        return newReceiveBuilder()
                .onMessage(AreaMessage.LoadComplete.class, msg -> {
                    for (MapObject obj : msg.objects()) {
                        state.addObject(obj);
                        spatialIndex.add(obj.getId(), obj.getGridId());
                    }
                    timers.startTimerWithFixedDelay("tick", new AreaMessage.Tick(), TICK_INTERVAL);
                    timers.startTimerWithFixedDelay("persist", new AreaMessage.Persist(), PERSIST_INTERVAL);
                    return stash.unstashAll(active());
                })
                .onMessage(AreaMessage.LoadFailed.class, msg -> {
                    getContext().getLog().error("Area {} load failed, retrying", areaId, msg.cause());
                    startAsyncLoad();
                    return Behaviors.same();
                })
                .onAnyMessage(msg -> {
                    stash.stash(msg);
                    return Behaviors.same();
                })
                .build();
    }

    private Receive<AreaMessage> active() {
        return newReceiveBuilder()
                .onMessage(AreaMessage.Tick.class, this::onTick)
                .onMessage(AreaMessage.Persist.class, this::onPersist)
                .onMessage(AreaMessage.PersistFailed.class, this::onPersistFailed)
                .onMessage(AreaMessage.Subscribe.class, this::onSubscribe)
                .onMessage(AreaMessage.Unsubscribe.class, this::onUnsubscribe)
                .onMessage(AreaMessage.UpdateSubscription.class, this::onUpdateSubscription)
                .onMessage(AreaMessage.MarchLineEnter.class, this::onMarchLineEnter)
                .onMessage(AreaMessage.MarchLineLeave.class, this::onMarchLineLeave)
                .onMessage(AreaMessage.AttackRequest.class, this::onAttack)
                .onMessage(AreaMessage.AddMapObject.class, this::onAddMapObject)
                .onMessage(AreaMessage.RemoveMapObject.class, this::onRemoveMapObject)
                .onMessage(AreaMessage.Flush.class, this::onFlush)
                .build();
    }

    private Behavior<AreaMessage> onTick(AreaMessage.Tick msg) {
        List<ViewEvent> batchEvents = new ArrayList<>();
        behavior.onTick(state, batchEvents);

        // 更新空间索引中变更的对象
        for (ViewEvent event : batchEvents) {
            if (event.changeType() == ChangeType.REMOVED) {
                spatialIndex.remove(event.objectId());
            }
        }

        if (!batchEvents.isEmpty()) {
            viewNotifier.notifyBatch(batchEvents);
            publishToStream(batchEvents);
        }
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onPersist(AreaMessage.Persist msg) {
        AreaState.PersistenceBatch batch = state.drainDirty();
        if (!batch.isEmpty()) {
            var self = getContext().getSelf();
            Set<Long> dirtyIds = new java.util.HashSet<>(batch.objects().keySet());
            Set<Long> removedIds = new java.util.HashSet<>(batch.removedObjectIds());
            CompletableFuture.runAsync(() -> {
                Map<String, byte[]> entries = new HashMap<>();
                for (var entry : batch.objects().entrySet()) {
                    entries.put(cacheKey(entry.getKey()), serializer.serialize(entry.getValue()));
                }
                if (!entries.isEmpty()) {
                    cache.putAll(entries);
                }
                if (!batch.removedObjectIds().isEmpty()) {
                    String[] keys = batch.removedObjectIds().stream()
                            .map(this::cacheKey).toArray(String[]::new);
                    cache.remove(keys);
                }
            }).whenComplete((v, ex) -> {
                if (ex != null) {
                    self.tell(new AreaMessage.PersistFailed(dirtyIds, removedIds));
                }
            });
        }
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onPersistFailed(AreaMessage.PersistFailed msg) {
        getContext().getLog().warn("Persist failed for area {}, re-marking {} dirty objects", areaId, msg.dirtyIds().size());
        state.reMarkDirty(msg.dirtyIds(), msg.removedIds());
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onSubscribe(AreaMessage.Subscribe msg) {
        viewNotifier.subscribe(msg.observer(), msg.gridIds());
        // 发送当前视野内对象的快照
        List<ViewEvent> snapshot = buildSnapshot(msg.gridIds());
        for (ViewEvent event : snapshot) {
            msg.observer().tell(new PlayerMessage.ViewEventMsg(event));
        }
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onUnsubscribe(AreaMessage.Unsubscribe msg) {
        viewNotifier.unsubscribe(msg.observer());
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onUpdateSubscription(AreaMessage.UpdateSubscription msg) {
        viewNotifier.updateSubscription(msg.observer(), msg.newGridIds());
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onMarchLineEnter(AreaMessage.MarchLineEnter msg) {
        state.addMarchLine(msg.marchId(), msg.lineInfo(), msg.grids());
        ViewEvent event = new ViewEvent(msg.marchId(), msg.lineInfo(), ChangeType.ADDED,
                msg.grids().iterator().next(), areaId);
        viewNotifier.notifyBatch(List.of(event));
        publishToStream(List.of(event));
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onMarchLineLeave(AreaMessage.MarchLineLeave msg) {
        Set<GridId> grids = state.getMarchLineGrids(msg.marchId());
        state.removeMarchLine(msg.marchId());
        if (!grids.isEmpty()) {
            ViewEvent event = new ViewEvent(msg.marchId(), null, ChangeType.REMOVED,
                    grids.iterator().next(), areaId);
            viewNotifier.notifyBatch(List.of(event));
            publishToStream(List.of(event));
        }
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onAttack(AreaMessage.AttackRequest msg) {
        behavior.onAttack(new AreaBehavior.AttackContext(
                msg.marchId(), msg.playerId(), msg.troops(), msg.targetId(), state));
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onAddMapObject(AreaMessage.AddMapObject msg) {
        state.addObject(msg.obj());
        spatialIndex.add(msg.obj().getId(), msg.obj().getGridId());
        if (msg.notifyView()) {
            ViewEvent event = new ViewEvent(msg.obj().getId(), msg.obj().snapshot(),
                    ChangeType.ADDED, msg.obj().getGridId(), areaId);
            viewNotifier.notifyBatch(List.of(event));
            publishToStream(List.of(event));
        }
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onRemoveMapObject(AreaMessage.RemoveMapObject msg) {
        MapObject removed = state.removeObject(msg.objectId());
        if (removed != null) {
            spatialIndex.remove(msg.objectId());
            if (msg.notifyView()) {
                ViewEvent event = new ViewEvent(removed.getId(), null,
                        ChangeType.REMOVED, removed.getGridId(), areaId);
                viewNotifier.notifyBatch(List.of(event));
                publishToStream(List.of(event));
            }
        }
        return Behaviors.same();
    }

    private Behavior<AreaMessage> onFlush(AreaMessage.Flush msg) {
        AreaState.PersistenceBatch batch = state.drainDirty();
        if (!batch.isEmpty()) {
            Map<String, byte[]> entries = new HashMap<>();
            for (var entry : batch.objects().entrySet()) {
                entries.put(cacheKey(entry.getKey()), serializer.serialize(entry.getValue()));
            }
            cache.putAll(entries);
            if (!batch.removedObjectIds().isEmpty()) {
                cache.remove(batch.removedObjectIds().stream().map(this::cacheKey).toArray(String[]::new));
            }
        }
        msg.ack().tell(new AreaMessage.FlushComplete(areaId));
        return Behaviors.same();
    }

    private void publishToStream(List<ViewEvent> events) {
        String streamKey = "scene:change:" + serverId + ":" + areaId;
        byte[] payload = serializer.serialize(events);
        streamPublisher.publish(streamKey, payload);
    }

    private List<ViewEvent> buildSnapshot(Set<GridId> gridIds) {
        List<ViewEvent> snapshot = new ArrayList<>();
        // 使用空间索引快速查找格子内的对象
        Set<Long> objectIds = spatialIndex.getObjectsIn(gridIds);
        for (long objectId : objectIds) {
            MapObject obj = state.getObjects().get(objectId);
            if (obj != null) {
                snapshot.add(new ViewEvent(obj.getId(), obj.snapshot(), ChangeType.ADDED, obj.getGridId(), areaId));
            }
        }
        // 包含经过订阅格子的行军路线
        for (var entry : state.getTransientLines().entrySet()) {
            Set<GridId> lineGrids = state.getMarchLineGrids(entry.getKey());
            for (GridId lineGrid : lineGrids) {
                if (gridIds.contains(lineGrid)) {
                    snapshot.add(new ViewEvent(entry.getKey(), entry.getValue(), ChangeType.ADDED, lineGrid, areaId));
                    break;
                }
            }
        }
        return snapshot;
    }

    private void startAsyncLoad() {
        var self = getContext().getSelf();
        CompletableFuture.supplyAsync(() -> behavior.generateInitialObjects(areaId))
                .whenComplete((objects, ex) -> {
                    if (ex != null) {
                        self.tell(new AreaMessage.LoadFailed(ex));
                    } else {
                        self.tell(new AreaMessage.LoadComplete(objects));
                    }
                });
    }

    private String cacheKey(long objectId) {
        return "server:" + serverId + ":scene:area:" + areaId + ":obj:" + objectId;
    }

    public AreaViewNotifier getViewNotifier() {
        return viewNotifier;
    }

    public SpatialIndex getSpatialIndex() {
        return spatialIndex;
    }
}
