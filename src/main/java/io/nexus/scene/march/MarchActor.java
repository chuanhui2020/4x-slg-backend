package io.nexus.actor.scene.march;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.router.ActorRouter;
import io.nexus.actor.scene.area.AreaMessage;
import io.nexus.actor.scene.area.AreaState;
import io.nexus.actor.view.GridId;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

public class MarchActor extends AbstractBehavior<MarchMessage> {

    private final TimerScheduler<MarchMessage> timers;
    private final MarchState state;
    private final MarchBehavior behavior;
    private final ActorRouter router;
    private final int serverId;
    private final ActorAddress replyTo;

    public static Behavior<MarchMessage> create(MarchMessage.Create params, MarchBehavior behavior,
                                                 ActorRouter router, int serverId) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new MarchActor(ctx, timers, params, behavior, router, serverId)));
    }

    public static Behavior<MarchMessage> createRecovered(long marchId, MarchMessage.Create params,
                                                          MarchBehavior behavior, ActorRouter router,
                                                          int serverId, Duration remainingTime) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new MarchActor(ctx, timers, marchId, params, behavior, router, serverId, remainingTime)));
    }

    private MarchActor(ActorContext<MarchMessage> context, TimerScheduler<MarchMessage> timers,
                        MarchMessage.Create params, MarchBehavior behavior,
                        ActorRouter router, int serverId) {
        super(context);
        this.timers = timers;
        this.behavior = behavior;
        this.router = router;
        this.serverId = serverId;
        this.replyTo = params.replyTo();

        long marchId = behavior.generateMarchId();
        this.state = new MarchState(marchId, params.playerId(), params.path(), params.speed(),
                params.targetId(), params.type(), params.troops());

        registerLines();
        Duration arrivalDelay = behavior.calcArrivalTime(state);
        timers.startSingleTimer("arrival", new MarchMessage.ArrivalTimeout(), arrivalDelay);
        router.tell(replyTo, new MarchMessage.MarchCreated(marchId));
    }

    private MarchActor(ActorContext<MarchMessage> context, TimerScheduler<MarchMessage> timers,
                        long marchId, MarchMessage.Create params, MarchBehavior behavior,
                        ActorRouter router, int serverId, Duration remainingTime) {
        super(context);
        this.timers = timers;
        this.behavior = behavior;
        this.router = router;
        this.serverId = serverId;
        this.replyTo = params.replyTo();

        this.state = new MarchState(marchId, params.playerId(), params.path(), params.speed(),
                params.targetId(), params.type(), params.troops());

        registerLines();
        timers.startSingleTimer("arrival", new MarchMessage.ArrivalTimeout(), remainingTime);
        context.getLog().info("Recovered march {} with {}ms remaining", marchId, remainingTime.toMillis());
    }

    @Override
    public Receive<MarchMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(MarchMessage.ArrivalTimeout.class, this::onArrival)
                .onMessage(MarchMessage.Recall.class, this::onRecall)
                .onMessage(MarchMessage.Intercept.class, this::onIntercept)
                .onMessage(MarchMessage.SpeedUp.class, this::onSpeedUp)
                .onMessage(MarchMessage.BattleResult.class, this::onBattleResult)
                .build();
    }

    private Behavior<MarchMessage> onArrival(MarchMessage.ArrivalTimeout msg) {
        behavior.onArrival(state);
        GridId target = state.getEndPoint();
        int targetAreaId = behavior.gridToAreaMapper().apply(target);
        router.tell(ActorAddress.area(targetAreaId, serverId),
                new AreaMessage.AttackRequest(state.getMarchId(), state.getPlayerId(),
                        state.getTroops(), state.getTargetId(), replyTo));
        return Behaviors.same();
    }

    private Behavior<MarchMessage> onRecall(MarchMessage.Recall msg) {
        timers.cancel("arrival");
        unregisterLines();
        behavior.onRecall(state);
        router.tell(msg.replyTo(), new MarchMessage.MarchRecalled(state.getMarchId()));
        return Behaviors.stopped();
    }

    private Behavior<MarchMessage> onIntercept(MarchMessage.Intercept msg) {
        timers.cancel("arrival");
        behavior.onIntercept(state, msg.attackerId(), msg.point());
        return Behaviors.same();
    }

    private Behavior<MarchMessage> onSpeedUp(MarchMessage.SpeedUp msg) {
        timers.cancel("arrival");
        Duration newDelay = behavior.recalcArrivalTime(state, msg.multiplier());
        timers.startSingleTimer("arrival", new MarchMessage.ArrivalTimeout(), newDelay);
        updateLines();
        return Behaviors.same();
    }

    private Behavior<MarchMessage> onBattleResult(MarchMessage.BattleResult msg) {
        unregisterLines();
        behavior.onBattleResult(state, msg.result());
        router.tell(replyTo, msg);

        if (behavior.shouldCreateReturnMarch(state, msg.result())) {
            MarchMessage.Create returnParams = behavior.buildReturnMarch(state);
            router.tell(ActorAddress.marchSupervisor(serverId), returnParams);
        }
        return Behaviors.stopped();
    }

    private void registerLines() {
        Map<Integer, Set<GridId>> areaGrids = state.calcAreaGrids(behavior.gridToAreaMapper());
        AreaState.MarchLineInfo lineInfo = behavior.toLineInfo(state);
        for (var entry : areaGrids.entrySet()) {
            router.tell(ActorAddress.area(entry.getKey(), serverId),
                    new AreaMessage.MarchLineEnter(state.getMarchId(), lineInfo, entry.getValue()));
        }
    }

    private void unregisterLines() {
        Map<Integer, Set<GridId>> areaGrids = state.calcAreaGrids(behavior.gridToAreaMapper());
        for (int areaId : areaGrids.keySet()) {
            router.tell(ActorAddress.area(areaId, serverId),
                    new AreaMessage.MarchLineLeave(state.getMarchId()));
        }
    }

    private void updateLines() {
        unregisterLines();
        registerLines();
    }
}
