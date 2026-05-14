package io.nexus.actor.scene;

import io.nexus.actor.address.ActorType;
import io.nexus.actor.persistence.CacheProvider;
import io.nexus.actor.persistence.StreamPublisher;
import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.router.ActorRouter;
import io.nexus.actor.scene.area.AreaActor;
import io.nexus.actor.scene.area.AreaBehavior;
import io.nexus.actor.scene.area.AreaMessage;
import io.nexus.actor.scene.event.EventSupervisor;
import io.nexus.actor.scene.march.MarchBehavior;
import io.nexus.actor.scene.march.MarchMessage;
import io.nexus.actor.scene.march.MarchSupervisor;
import io.nexus.actor.scene.rally.RallyBehavior;
import io.nexus.actor.scene.rally.RallyMessage;
import io.nexus.actor.scene.rally.RallySupervisor;
import io.nexus.actor.serialize.RpcSerializer;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

public class MapSceneActor extends AbstractBehavior<SceneMessage> {

    private final ActorRegistry registry;
    private final ActorRouter router;
    private final CacheProvider cache;
    private final StreamPublisher streamPublisher;
    private final RpcSerializer serializer;
    private final int serverId;
    private final AreaBehavior areaBehavior;
    private final MarchBehavior marchBehavior;
    private final RallyBehavior rallyBehavior;

    public static Behavior<SceneMessage> create(ActorRegistry registry, ActorRouter router,
                                                 CacheProvider cache, StreamPublisher streamPublisher,
                                                 RpcSerializer serializer, int serverId,
                                                 AreaBehavior areaBehavior, MarchBehavior marchBehavior,
                                                 RallyBehavior rallyBehavior) {
        return Behaviors.setup(ctx -> new MapSceneActor(ctx, registry, router, cache, streamPublisher,
                serializer, serverId, areaBehavior, marchBehavior, rallyBehavior));
    }

    private MapSceneActor(ActorContext<SceneMessage> context, ActorRegistry registry, ActorRouter router,
                           CacheProvider cache, StreamPublisher streamPublisher, RpcSerializer serializer,
                           int serverId, AreaBehavior areaBehavior, MarchBehavior marchBehavior,
                           RallyBehavior rallyBehavior) {
        super(context);
        this.registry = registry;
        this.router = router;
        this.cache = cache;
        this.streamPublisher = streamPublisher;
        this.serializer = serializer;
        this.serverId = serverId;
        this.areaBehavior = areaBehavior;
        this.marchBehavior = marchBehavior;
        this.rallyBehavior = rallyBehavior;
    }

    @Override
    public Receive<SceneMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(SceneMessage.Init.class, this::onInit)
                .onMessage(SceneMessage.Shutdown.class, this::onShutdown)
                .build();
    }

    private Behavior<SceneMessage> onInit(SceneMessage.Init msg) {
        int areaCount = msg.areaCount();

        // Spawn AreaActors
        for (int i = 0; i < areaCount; i++) {
            Behavior<AreaMessage> areaBeh = AreaActor.create(i, serverId, areaBehavior, cache,
                    streamPublisher, router, serializer);
            ActorRef<AreaMessage> areaRef = getContext().spawn(areaBeh, "area-" + i);
            registry.register(ActorType.AREA, i, areaRef);
        }

        // Spawn MarchSupervisor
        Behavior<MarchMessage> marchBeh = MarchSupervisor.create(registry, router, marchBehavior, serverId);
        ActorRef<MarchMessage> marchRef = getContext().spawn(marchBeh, "march-supervisor");
        registry.register(ActorType.MARCH_SUPERVISOR, 0, marchRef);

        // Spawn RallySupervisor
        Behavior<RallyMessage> rallyBeh = RallySupervisor.create(registry, router, rallyBehavior, serverId);
        ActorRef<RallyMessage> rallyRef = getContext().spawn(rallyBeh, "rally-supervisor");
        registry.register(ActorType.RALLY_SUPERVISOR, 0, rallyRef);

        // Spawn EventSupervisor
        Behavior<EventSupervisor.EventCommand> eventBeh = EventSupervisor.create(registry, router, cache);
        ActorRef<EventSupervisor.EventCommand> eventRef = getContext().spawn(eventBeh, "event-supervisor");
        registry.register(ActorType.EVENT, 0, eventRef);

        getContext().getLog().info("MapSceneActor initialized with {} areas", areaCount);
        return Behaviors.same();
    }

    private Behavior<SceneMessage> onShutdown(SceneMessage.Shutdown msg) {
        getContext().getLog().info("MapSceneActor shutting down");
        // Flush all area actors
        // In production, coordinate flush with GracefulShutdown
        msg.ack().tell(new SceneMessage.ShutdownComplete());
        return Behaviors.stopped();
    }
}
