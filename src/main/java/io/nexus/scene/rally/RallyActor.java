package io.nexus.actor.scene.rally;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;

public class RallyActor extends AbstractBehavior<RallyMessage> {

    private final TimerScheduler<RallyMessage> timers;
    private final RallyState state;
    private final RallyBehavior behavior;
    private final ActorRouter router;
    private final int serverId;

    public static Behavior<RallyMessage> create(long rallyId, RallyMessage.Create params,
                                                 RallyBehavior behavior, ActorRouter router, int serverId) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new RallyActor(ctx, timers, rallyId, params, behavior, router, serverId)));
    }

    private RallyActor(ActorContext<RallyMessage> context, TimerScheduler<RallyMessage> timers,
                        long rallyId, RallyMessage.Create params, RallyBehavior behavior,
                        ActorRouter router, int serverId) {
        super(context);
        this.timers = timers;
        this.behavior = behavior;
        this.router = router;
        this.serverId = serverId;
        this.state = new RallyState(rallyId, params.initiatorId(), params.targetId(),
                params.path(), params.maxMembers());

        timers.startSingleTimer("wait-timeout", new RallyMessage.WaitTimeout(),
                Duration.ofMillis(params.timeoutMs()));
    }

    @Override
    public Receive<RallyMessage> createReceive() {
        return waiting();
    }

    private Receive<RallyMessage> waiting() {
        return newReceiveBuilder()
                .onMessage(RallyMessage.Join.class, this::onJoin)
                .onMessage(RallyMessage.Leave.class, this::onLeave)
                .onMessage(RallyMessage.WaitTimeout.class, this::onWaitTimeout)
                .onMessage(RallyMessage.Depart.class, this::onDepart)
                .onMessage(RallyMessage.Dismiss.class, this::onDismiss)
                .build();
    }

    private Receive<RallyMessage> marching() {
        return newReceiveBuilder()
                .onMessage(RallyMessage.ArrivalTimeout.class, this::onArrival)
                .onMessage(RallyMessage.Dismiss.class, this::onDismiss)
                .build();
    }

    private Receive<RallyMessage> battling() {
        return newReceiveBuilder()
                .onMessage(RallyMessage.BattleResult.class, this::onBattleResult)
                .onMessage(RallyMessage.Dismiss.class, this::onDismiss)
                .build();
    }

    private Behavior<RallyMessage> onJoin(RallyMessage.Join msg) {
        if (!behavior.canJoin(state, msg.playerId())) {
            router.tell(msg.replyTo(), new RallyMessage.JoinResult(state.getRallyId(), false, "Cannot join"));
            return Behaviors.same();
        }
        if (state.isFull()) {
            router.tell(msg.replyTo(), new RallyMessage.JoinResult(state.getRallyId(), false, "Rally is full"));
            return Behaviors.same();
        }
        behavior.onJoin(state, msg.playerId(), msg.troops());
        state.addMember(msg.playerId(), msg.troops());
        router.tell(msg.replyTo(), new RallyMessage.JoinResult(state.getRallyId(), true, null));

        if (state.isFull()) {
            return startMarch();
        }
        return Behaviors.same();
    }

    private Behavior<RallyMessage> onLeave(RallyMessage.Leave msg) {
        if (!state.hasMember(msg.playerId())) {
            router.tell(msg.replyTo(), new RallyMessage.LeaveResult(state.getRallyId(), false));
            return Behaviors.same();
        }
        behavior.onLeave(state, msg.playerId());
        state.removeMember(msg.playerId());
        router.tell(msg.replyTo(), new RallyMessage.LeaveResult(state.getRallyId(), true));

        if (state.getMembers().isEmpty()) {
            return dismiss();
        }
        return Behaviors.same();
    }

    private Behavior<RallyMessage> onWaitTimeout(RallyMessage.WaitTimeout msg) {
        if (state.getMembers().isEmpty()) {
            return dismiss();
        }
        return startMarch();
    }

    private Behavior<RallyMessage> onDepart(RallyMessage.Depart msg) {
        return startMarch();
    }

    private Behavior<RallyMessage> onArrival(RallyMessage.ArrivalTimeout msg) {
        state.setPhase(RallyState.Phase.BATTLING);
        behavior.onArrival(state);
        return battling();
    }

    private Behavior<RallyMessage> onBattleResult(RallyMessage.BattleResult msg) {
        state.setPhase(RallyState.Phase.RETURNING);
        behavior.onBattleResult(state, msg.result());
        return dismiss();
    }

    private Behavior<RallyMessage> onDismiss(RallyMessage.Dismiss msg) {
        return dismiss();
    }

    private Behavior<RallyMessage> startMarch() {
        timers.cancel("wait-timeout");
        state.setPhase(RallyState.Phase.MARCHING);
        behavior.onDepart(state);
        Duration marchDuration = behavior.calcMarchDuration(state);
        timers.startSingleTimer("arrival", new RallyMessage.ArrivalTimeout(), marchDuration);
        return marching();
    }

    private Behavior<RallyMessage> dismiss() {
        state.setPhase(RallyState.Phase.DISMISSED);
        behavior.onDismiss(state);
        for (var member : state.getMembers().values()) {
            router.tell(ActorAddress.player(member.playerId(), serverId),
                    new RallyMessage.RallyDismissed(state.getRallyId()));
        }
        return Behaviors.stopped();
    }
}
