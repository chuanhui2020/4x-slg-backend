package io.nexus.actor.scene.event;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.persistence.CacheProvider;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.*;

public class GuildWarActor extends AbstractBehavior<GuildWarMessage> {

    private final TimerScheduler<GuildWarMessage> timers;
    private final long warId;
    private final ActorRouter router;
    private final CacheProvider cache;
    private final GuildWarBehavior behavior;

    private int currentPhase;
    private final Map<Long, Integer> guildScores = new HashMap<>();
    private final Map<Integer, Long> pointOwnership = new HashMap<>();
    private final List<Long> participatingGuilds = new ArrayList<>();

    public static Behavior<GuildWarMessage> create(long warId, GuildWarBehavior behavior,
                                                    ActorRouter router, CacheProvider cache) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new GuildWarActor(ctx, timers, warId, behavior, router, cache)));
    }

    private GuildWarActor(ActorContext<GuildWarMessage> context, TimerScheduler<GuildWarMessage> timers,
                           long warId, GuildWarBehavior behavior, ActorRouter router, CacheProvider cache) {
        super(context);
        this.timers = timers;
        this.warId = warId;
        this.behavior = behavior;
        this.router = router;
        this.cache = cache;
        this.currentPhase = 0;
    }

    @Override
    public Receive<GuildWarMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(GuildWarMessage.Start.class, this::onStart)
                .onMessage(GuildWarMessage.Tick.class, this::onTick)
                .onMessage(GuildWarMessage.CapturePoint.class, this::onCapture)
                .onMessage(GuildWarMessage.PhaseChange.class, this::onPhaseChange)
                .onMessage(GuildWarMessage.End.class, this::onEnd)
                .onMessage(GuildWarMessage.GetScore.class, this::onGetScore)
                .onMessage(GuildWarMessage.Flush.class, this::onFlush)
                .build();
    }

    private Behavior<GuildWarMessage> onStart(GuildWarMessage.Start msg) {
        participatingGuilds.addAll(msg.participatingGuilds());
        for (long guildId : participatingGuilds) {
            guildScores.put(guildId, 0);
        }
        currentPhase = 1;
        Duration tickInterval = behavior.tickInterval();
        timers.startTimerWithFixedDelay("tick", new GuildWarMessage.Tick(), tickInterval);
        behavior.onStart(warId, participatingGuilds);
        return Behaviors.same();
    }

    private Behavior<GuildWarMessage> onTick(GuildWarMessage.Tick msg) {
        behavior.onTick(warId, currentPhase, guildScores, pointOwnership);
        // Award points for held positions
        for (var entry : pointOwnership.entrySet()) {
            int pointScore = behavior.getPointScore(entry.getKey());
            guildScores.merge(entry.getValue(), pointScore, Integer::sum);
        }
        return Behaviors.same();
    }

    private Behavior<GuildWarMessage> onCapture(GuildWarMessage.CapturePoint msg) {
        boolean success = behavior.canCapture(warId, msg.playerId(), msg.guildId(), msg.pointId(), pointOwnership);
        if (success) {
            pointOwnership.put(msg.pointId(), msg.guildId());
            behavior.onCapture(warId, msg.pointId(), msg.guildId());
        }
        router.tell(msg.replyTo(), new GuildWarMessage.CaptureResult(success, msg.pointId(), msg.guildId()));
        return Behaviors.same();
    }

    private Behavior<GuildWarMessage> onPhaseChange(GuildWarMessage.PhaseChange msg) {
        currentPhase = msg.newPhase();
        behavior.onPhaseChange(warId, currentPhase);
        return Behaviors.same();
    }

    private Behavior<GuildWarMessage> onEnd(GuildWarMessage.End msg) {
        timers.cancel("tick");
        behavior.onEnd(warId, guildScores);
        return Behaviors.stopped();
    }

    private Behavior<GuildWarMessage> onGetScore(GuildWarMessage.GetScore msg) {
        msg.replyTo().tell(new GuildWarMessage.ScoreResponse(warId, new HashMap<>(guildScores)));
        return Behaviors.same();
    }

    private Behavior<GuildWarMessage> onFlush(GuildWarMessage.Flush msg) {
        behavior.onFlush(warId, currentPhase, guildScores, pointOwnership);
        msg.ack().tell(new GuildWarMessage.FlushComplete(warId));
        return Behaviors.same();
    }

    public interface GuildWarBehavior {
        Duration tickInterval();
        void onStart(long warId, List<Long> guilds);
        void onTick(long warId, int phase, Map<Long, Integer> scores, Map<Integer, Long> ownership);
        boolean canCapture(long warId, long playerId, long guildId, int pointId, Map<Integer, Long> ownership);
        void onCapture(long warId, int pointId, long guildId);
        void onPhaseChange(long warId, int phase);
        void onEnd(long warId, Map<Long, Integer> finalScores);
        int getPointScore(int pointId);
        void onFlush(long warId, int phase, Map<Long, Integer> scores, Map<Integer, Long> ownership);
    }
}
