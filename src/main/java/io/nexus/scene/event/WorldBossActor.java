package io.nexus.actor.scene.event;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.persistence.CacheProvider;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.*;

public class WorldBossActor extends AbstractBehavior<WorldBossMessage> {

    private final TimerScheduler<WorldBossMessage> timers;
    private final long bossId;
    private final ActorRouter router;
    private final CacheProvider cache;
    private final WorldBossBehavior behavior;

    private long maxHp;
    private long currentHp;
    private int currentPhase;
    private final Map<Long, Long> damageRanking = new LinkedHashMap<>();

    public static Behavior<WorldBossMessage> create(long bossId, WorldBossBehavior behavior,
                                                     ActorRouter router, CacheProvider cache) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new WorldBossActor(ctx, timers, bossId, behavior, router, cache)));
    }

    private WorldBossActor(ActorContext<WorldBossMessage> context, TimerScheduler<WorldBossMessage> timers,
                            long bossId, WorldBossBehavior behavior, ActorRouter router, CacheProvider cache) {
        super(context);
        this.timers = timers;
        this.bossId = bossId;
        this.behavior = behavior;
        this.router = router;
        this.cache = cache;
        this.maxHp = behavior.getMaxHp(bossId);
        this.currentHp = maxHp;
        this.currentPhase = 0;

        Duration tickInterval = behavior.tickInterval();
        if (tickInterval != null) {
            timers.startTimerWithFixedDelay("tick", new WorldBossMessage.Tick(), tickInterval);
        }
    }

    @Override
    public Receive<WorldBossMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(WorldBossMessage.Attack.class, this::onAttack)
                .onMessage(WorldBossMessage.Tick.class, this::onTick)
                .onMessage(WorldBossMessage.GetRanking.class, this::onGetRanking)
                .onMessage(WorldBossMessage.Flush.class, this::onFlush)
                .build();
    }

    private Behavior<WorldBossMessage> onAttack(WorldBossMessage.Attack msg) {
        if (currentHp <= 0) {
            return Behaviors.same();
        }
        long damage = behavior.calcDamage(bossId, msg.playerId(), msg.troops(), currentHp);
        currentHp = Math.max(0, currentHp - damage);
        damageRanking.merge(msg.playerId(), damage, Long::sum);

        router.tell(msg.replyTo(), new WorldBossMessage.AttackResult(msg.playerId(), damage, currentHp));

        int newPhase = behavior.calcPhase(maxHp, currentHp);
        if (newPhase != currentPhase) {
            currentPhase = newPhase;
            behavior.onPhaseChange(bossId, currentPhase, damageRanking);
        }

        if (currentHp <= 0) {
            behavior.onBossDead(bossId, damageRanking);
            timers.cancel("tick");
        }
        return Behaviors.same();
    }

    private Behavior<WorldBossMessage> onTick(WorldBossMessage.Tick msg) {
        behavior.onTick(bossId, currentHp, currentPhase);
        return Behaviors.same();
    }

    private Behavior<WorldBossMessage> onGetRanking(WorldBossMessage.GetRanking msg) {
        List<WorldBossMessage.RankEntry> ranking = new ArrayList<>();
        List<Map.Entry<Long, Long>> sorted = damageRanking.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByValue().reversed())
                .toList();
        for (int i = 0; i < sorted.size(); i++) {
            var entry = sorted.get(i);
            ranking.add(new WorldBossMessage.RankEntry(entry.getKey(), entry.getValue(), i + 1));
        }
        msg.replyTo().tell(new WorldBossMessage.RankingResponse(bossId, ranking));
        return Behaviors.same();
    }

    private Behavior<WorldBossMessage> onFlush(WorldBossMessage.Flush msg) {
        behavior.onFlush(bossId, currentHp, currentPhase, damageRanking);
        msg.ack().tell(new WorldBossMessage.FlushComplete(bossId));
        return Behaviors.same();
    }

    public interface WorldBossBehavior {
        long getMaxHp(long bossId);
        Duration tickInterval();
        long calcDamage(long bossId, long playerId, Object troops, long currentHp);
        int calcPhase(long maxHp, long currentHp);
        void onPhaseChange(long bossId, int phase, Map<Long, Long> damageRanking);
        void onBossDead(long bossId, Map<Long, Long> damageRanking);
        void onTick(long bossId, long currentHp, int phase);
        void onFlush(long bossId, long currentHp, int phase, Map<Long, Long> damageRanking);
    }
}
