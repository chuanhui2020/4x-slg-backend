package io.nexus.actor.player;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.persistence.StreamConsumer;
import io.nexus.actor.router.ActorRouter;
import io.nexus.actor.scene.aoi.AoiCalculator;
import io.nexus.actor.scene.aoi.AoiDiff;
import io.nexus.actor.scene.aoi.AoiManager;
import io.nexus.actor.scene.area.AreaMessage;
import io.nexus.actor.view.GridId;
import io.nexus.actor.view.ViewEvent;
import io.nexus.actor.view.ViewThrottler;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.*;

public class PlayerActor extends AbstractBehavior<PlayerMessage> {

    private static final int MAX_PENDING_EVENTS = 200;
    private static final Duration FLUSH_INTERVAL = Duration.ofMillis(100);

    private final TimerScheduler<PlayerMessage> timers;
    private final long playerId;
    private final PlayerSession session;
    private final boolean isMaster;
    private final ActorRouter router;
    private final StreamConsumer streamConsumer;
    private final int serverId;
    private final ViewThrottler viewThrottler;
    private final PlayerMessageHandler messageHandler;
    private final AoiManager aoiManager;

    private int currentSceneServerId;

    public static Behavior<PlayerMessage> create(long playerId, PlayerSession session, boolean isMaster,
                                                  ActorRouter router, StreamConsumer streamConsumer,
                                                  int serverId, PlayerMessageHandler messageHandler,
                                                  AoiCalculator aoiCalculator) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new PlayerActor(ctx, timers, playerId, session, isMaster,
                        router, streamConsumer, serverId, messageHandler, aoiCalculator)));
    }

    private PlayerActor(ActorContext<PlayerMessage> context, TimerScheduler<PlayerMessage> timers,
                         long playerId, PlayerSession session, boolean isMaster,
                         ActorRouter router, StreamConsumer streamConsumer,
                         int serverId, PlayerMessageHandler messageHandler,
                         AoiCalculator aoiCalculator) {
        super(context);
        this.timers = timers;
        this.playerId = playerId;
        this.session = session;
        this.isMaster = isMaster;
        this.router = router;
        this.streamConsumer = streamConsumer;
        this.serverId = serverId;
        this.currentSceneServerId = serverId;
        this.viewThrottler = new ViewThrottler(MAX_PENDING_EVENTS);
        this.messageHandler = messageHandler;
        this.aoiManager = new AoiManager(aoiCalculator);
    }

    @Override
    public Receive<PlayerMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(PlayerMessage.SessionMsg.class, this::onSessionMessage)
                .onMessage(PlayerMessage.RunTask.class, this::onRunTask)
                .onMessage(PlayerMessage.ViewEventMsg.class, this::onViewEvent)
                .onMessage(PlayerMessage.FlushView.class, this::onFlushView)
                .onMessage(PlayerMessage.OpenMap.class, this::onOpenMap)
                .onMessage(PlayerMessage.CloseMap.class, this::onCloseMap)
                .onMessage(PlayerMessage.MoveView.class, this::onMoveView)
                .onMessage(PlayerMessage.SceneWriteResult.class, this::onSceneWriteResult)
                .onMessage(PlayerMessage.Disconnect.class, this::onDisconnect)
                .onMessage(PlayerMessage.Stop.class, msg -> Behaviors.stopped())
                .build();
    }

    private Behavior<PlayerMessage> onSessionMessage(PlayerMessage.SessionMsg msg) {
        if (!session.isConnected()) {
            return Behaviors.same();
        }
        messageHandler.handleRequest(playerId, msg.requestData(), session);
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onRunTask(PlayerMessage.RunTask msg) {
        msg.task().run();
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onViewEvent(PlayerMessage.ViewEventMsg msg) {
        viewThrottler.addEvent(msg.event());
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onFlushView(PlayerMessage.FlushView msg) {
        List<ViewEvent> events = viewThrottler.flush();
        if (!events.isEmpty()) {
            byte[] data = messageHandler.buildViewUpdate(events);
            session.send(data);
        }
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onOpenMap(PlayerMessage.OpenMap msg) {
        Map<Integer, Set<GridId>> subscribed = aoiManager.openMap(msg.viewLB(), msg.viewRT());

        if (isMaster) {
            subscribed.forEach((areaId, grids) ->
                    router.tell(ActorAddress.area(areaId, currentSceneServerId),
                            new AreaMessage.Subscribe(getContext().getSelf(), grids)));
        } else {
            subscribed.keySet().forEach(areaId -> {
                String streamKey = "scene:change:" + currentSceneServerId + ":" + areaId;
                streamConsumer.subscribe(streamKey, this::handleStreamData);
            });
        }
        timers.startTimerWithFixedDelay("flush", new PlayerMessage.FlushView(), FLUSH_INTERVAL);
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onCloseMap(PlayerMessage.CloseMap msg) {
        timers.cancel("flush");
        Map<Integer, Set<GridId>> oldGrids = aoiManager.closeMap();

        if (!oldGrids.isEmpty()) {
            if (isMaster) {
                oldGrids.keySet().forEach(areaId ->
                        router.tell(ActorAddress.area(areaId, currentSceneServerId),
                                new AreaMessage.Unsubscribe(getContext().getSelf())));
            } else {
                oldGrids.keySet().forEach(areaId -> {
                    String streamKey = "scene:change:" + currentSceneServerId + ":" + areaId;
                    streamConsumer.unsubscribe(streamKey);
                });
            }
        }
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onMoveView(PlayerMessage.MoveView msg) {
        if (!aoiManager.isMapOpen()) return Behaviors.same();

        AoiDiff diff = aoiManager.moveView(msg.viewLB(), msg.viewRT());
        if (!diff.hasChanges()) return Behaviors.same();

        if (isMaster) {
            // 完全新增的区域：发送 Subscribe
            for (int areaId : diff.newAreaIds()) {
                Set<GridId> grids = diff.current().get(areaId);
                if (grids != null) {
                    router.tell(ActorAddress.area(areaId, currentSceneServerId),
                            new AreaMessage.Subscribe(getContext().getSelf(), grids));
                }
            }
            // 完全移除的区域：发送 Unsubscribe
            for (int areaId : diff.removedAreaIds()) {
                router.tell(ActorAddress.area(areaId, currentSceneServerId),
                        new AreaMessage.Unsubscribe(getContext().getSelf()));
            }
            // 部分变更的区域：发送 UpdateSubscription（完整的当前 Grid 集合）
            for (var entry : diff.current().entrySet()) {
                int areaId = entry.getKey();
                if (!diff.newAreaIds().contains(areaId) && !diff.removedAreaIds().contains(areaId)) {
                    if (diff.added().containsKey(areaId) || diff.removed().containsKey(areaId)) {
                        router.tell(ActorAddress.area(areaId, currentSceneServerId),
                                new AreaMessage.UpdateSubscription(getContext().getSelf(), entry.getValue()));
                    }
                }
            }
        } else {
            // Slave: 管理 Stream 订阅（按 Area 粒度订阅 Stream，Grid 级过滤在接收端）
            for (int areaId : diff.newAreaIds()) {
                streamConsumer.subscribe("scene:change:" + currentSceneServerId + ":" + areaId, this::handleStreamData);
            }
            for (int areaId : diff.removedAreaIds()) {
                streamConsumer.unsubscribe("scene:change:" + currentSceneServerId + ":" + areaId);
            }
        }
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onSceneWriteResult(PlayerMessage.SceneWriteResult msg) {
        messageHandler.handleSceneResult(playerId, msg.result(), session);
        return Behaviors.same();
    }

    private Behavior<PlayerMessage> onDisconnect(PlayerMessage.Disconnect msg) {
        if (aoiManager.isMapOpen()) {
            onCloseMap(new PlayerMessage.CloseMap());
        }
        session.close();
        return Behaviors.stopped();
    }

    public void executeSceneWrite(Object msg) {
        router.tell(ActorAddress.marchSupervisor(currentSceneServerId), msg);
    }

    public void setCurrentSceneServerId(int sceneServerId) {
        this.currentSceneServerId = sceneServerId;
    }

    public int getCurrentSceneServerId() {
        return currentSceneServerId;
    }

    public AoiManager getAoiManager() {
        return aoiManager;
    }

    private void handleStreamData(byte[] data) {
        List<ViewEvent> events = messageHandler.deserializeViewEvents(data);
        for (ViewEvent event : events) {
            if (aoiManager.isGridVisible(event.gridId())) {
                getContext().getSelf().tell(new PlayerMessage.ViewEventMsg(event));
            }
        }
    }
}
