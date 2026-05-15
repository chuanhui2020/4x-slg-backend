package io.nexus.actor.scene.march;

import io.nexus.actor.address.ActorType;
import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.List;

public class MarchSupervisor extends AbstractBehavior<MarchMessage> {

    private final ActorRegistry registry;
    private final ActorRouter router;
    private final MarchBehavior behavior;
    private final MarchStateStore stateStore;
    private final int serverId;

    public static Behavior<MarchMessage> create(ActorRegistry registry, ActorRouter router,
                                                 MarchBehavior behavior, int serverId) {
        return create(registry, router, behavior, serverId, null);
    }

    public static Behavior<MarchMessage> create(ActorRegistry registry, ActorRouter router,
                                                 MarchBehavior behavior, int serverId,
                                                 MarchStateStore stateStore) {
        return Behaviors.setup(ctx -> new MarchSupervisor(ctx, registry, router, behavior, serverId, stateStore));
    }

    private MarchSupervisor(ActorContext<MarchMessage> context, ActorRegistry registry,
                             ActorRouter router, MarchBehavior behavior, int serverId,
                             MarchStateStore stateStore) {
        super(context);
        this.registry = registry;
        this.router = router;
        this.behavior = behavior;
        this.serverId = serverId;
        this.stateStore = stateStore;

        if (stateStore != null) {
            recoverMarches();
        }
    }

    @Override
    public Receive<MarchMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(MarchMessage.Create.class, this::onCreate)
                .onMessage(MarchMessage.MarchCompleted.class, this::onCompleted)
                .build();
    }

    private Behavior<MarchMessage> onCreate(MarchMessage.Create msg) {
        try {
            Behavior<MarchMessage> marchBehavior = MarchActor.create(msg, behavior, router, serverId);
            ActorRef<MarchMessage> marchRef = getContext().spawnAnonymous(marchBehavior);
            long marchId = behavior.generateMarchId();
            registry.register(ActorType.EVENT, marchId, marchRef);

            if (stateStore != null) {
                long arrivalTime = System.currentTimeMillis() + behavior.calcArrivalTime(
                        new MarchState(marchId, msg.playerId(), msg.path(), msg.speed(),
                                msg.targetId(), msg.type(), msg.troops())).toMillis();
                String replyPodId = msg.replyTo() != null ? msg.replyTo().podId() : null;
                stateStore.save(new MarchStateStore.MarchSnapshot(
                        marchId, msg.playerId(), msg.path(), msg.speed(),
                        msg.targetId(), msg.type(), msg.troops(),
                        System.currentTimeMillis(), arrivalTime, replyPodId));
            }
        } catch (Exception e) {
            getContext().getLog().error("Failed to create march for player {}", msg.playerId(), e);
            if (msg.replyTo() != null) {
                router.tell(msg.replyTo(), new MarchMessage.MarchFailed(e.getMessage()));
            }
        }
        return Behaviors.same();
    }

    private Behavior<MarchMessage> onCompleted(MarchMessage.MarchCompleted msg) {
        if (stateStore != null) {
            stateStore.remove(msg.marchId());
        }
        registry.unregister(ActorType.EVENT, msg.marchId());
        return Behaviors.same();
    }

    private void recoverMarches() {
        List<MarchStateStore.MarchSnapshot> snapshots = stateStore.loadAll();
        long now = System.currentTimeMillis();
        int recovered = 0;
        int expired = 0;

        for (var snapshot : snapshots) {
            long remainingMs = snapshot.expectedArrivalTime() - now;
            if (remainingMs <= 0) {
                // 已过期的行军，直接触发到达逻辑
                behavior.onRecoveredArrival(snapshot);
                stateStore.remove(snapshot.marchId());
                expired++;
            } else {
                // 恢复行军 Actor，设置剩余时间的定时器
                MarchMessage.Create params = new MarchMessage.Create(
                        snapshot.playerId(), snapshot.path(), snapshot.speed(),
                        snapshot.targetId(), snapshot.type(), snapshot.troops(), null);
                Behavior<MarchMessage> marchBeh = MarchActor.createRecovered(
                        snapshot.marchId(), params, behavior, router, serverId,
                        Duration.ofMillis(remainingMs));
                ActorRef<MarchMessage> marchRef = getContext().spawnAnonymous(marchBeh);
                registry.register(ActorType.EVENT, snapshot.marchId(), marchRef);
                recovered++;
            }
        }

        if (recovered > 0 || expired > 0) {
            getContext().getLog().info("March recovery: {} restored, {} expired (arrived during downtime)", recovered, expired);
        }
    }
}
