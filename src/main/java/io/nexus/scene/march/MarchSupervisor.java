package io.nexus.actor.scene.march;

import io.nexus.actor.address.ActorType;
import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

public class MarchSupervisor extends AbstractBehavior<MarchMessage> {

    private final ActorRegistry registry;
    private final ActorRouter router;
    private final MarchBehavior behavior;
    private final int serverId;

    public static Behavior<MarchMessage> create(ActorRegistry registry, ActorRouter router,
                                                 MarchBehavior behavior, int serverId) {
        return Behaviors.setup(ctx -> new MarchSupervisor(ctx, registry, router, behavior, serverId));
    }

    private MarchSupervisor(ActorContext<MarchMessage> context, ActorRegistry registry,
                             ActorRouter router, MarchBehavior behavior, int serverId) {
        super(context);
        this.registry = registry;
        this.router = router;
        this.behavior = behavior;
        this.serverId = serverId;
    }

    @Override
    public Receive<MarchMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(MarchMessage.Create.class, this::onCreate)
                .build();
    }

    private Behavior<MarchMessage> onCreate(MarchMessage.Create msg) {
        try {
            Behavior<MarchMessage> marchBehavior = MarchActor.create(msg, behavior, router, serverId);
            ActorRef<MarchMessage> marchRef = getContext().spawnAnonymous(marchBehavior);
            long marchId = behavior.generateMarchId();
            registry.register(ActorType.EVENT, marchId, marchRef);
        } catch (Exception e) {
            getContext().getLog().error("Failed to create march for player {}", msg.playerId(), e);
            if (msg.replyTo() != null) {
                router.tell(msg.replyTo(), new MarchMessage.MarchFailed(e.getMessage()));
            }
        }
        return Behaviors.same();
    }
}
