package io.nexus.actor.scene.rally;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.address.ActorType;
import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

public class RallySupervisor extends AbstractBehavior<RallyMessage> {

    private final ActorRegistry registry;
    private final ActorRouter router;
    private final RallyBehavior behavior;
    private final int serverId;

    public static Behavior<RallyMessage> create(ActorRegistry registry, ActorRouter router,
                                                 RallyBehavior behavior, int serverId) {
        return Behaviors.setup(ctx -> new RallySupervisor(ctx, registry, router, behavior, serverId));
    }

    private RallySupervisor(ActorContext<RallyMessage> context, ActorRegistry registry,
                             ActorRouter router, RallyBehavior behavior, int serverId) {
        super(context);
        this.registry = registry;
        this.router = router;
        this.behavior = behavior;
        this.serverId = serverId;
    }

    @Override
    public Receive<RallyMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(RallyMessage.Create.class, this::onCreate)
                .build();
    }

    private Behavior<RallyMessage> onCreate(RallyMessage.Create msg) {
        try {
            long rallyId = behavior.generateRallyId();
            Behavior<RallyMessage> rallyBehavior = RallyActor.create(rallyId, msg, behavior, router, serverId);
            ActorRef<RallyMessage> rallyRef = getContext().spawnAnonymous(rallyBehavior);
            registry.register(ActorType.EVENT, rallyId, rallyRef);
            router.tell(msg.replyTo(), new RallyMessage.RallyCreated(rallyId));
        } catch (Exception e) {
            getContext().getLog().error("Failed to create rally for player {}", msg.initiatorId(), e);
        }
        return Behaviors.same();
    }
}
