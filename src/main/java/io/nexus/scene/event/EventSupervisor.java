package io.nexus.actor.scene.event;

import io.nexus.actor.address.ActorType;
import io.nexus.actor.persistence.CacheProvider;
import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

public class EventSupervisor extends AbstractBehavior<EventSupervisor.EventCommand> {

    public sealed interface EventCommand {
        record CreateWorldBoss(long bossId, WorldBossActor.WorldBossBehavior behavior) implements EventCommand {}
        record CreateGuildWar(long warId, GuildWarActor.GuildWarBehavior behavior) implements EventCommand {}
        record StopEvent(long eventId) implements EventCommand {}
    }

    private final ActorRegistry registry;
    private final ActorRouter router;
    private final CacheProvider cache;

    public static Behavior<EventCommand> create(ActorRegistry registry, ActorRouter router, CacheProvider cache) {
        return Behaviors.setup(ctx -> new EventSupervisor(ctx, registry, router, cache));
    }

    private EventSupervisor(ActorContext<EventCommand> context, ActorRegistry registry,
                             ActorRouter router, CacheProvider cache) {
        super(context);
        this.registry = registry;
        this.router = router;
        this.cache = cache;
    }

    @Override
    public Receive<EventCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(EventCommand.CreateWorldBoss.class, this::onCreateWorldBoss)
                .onMessage(EventCommand.CreateGuildWar.class, this::onCreateGuildWar)
                .onMessage(EventCommand.StopEvent.class, this::onStopEvent)
                .build();
    }

    private Behavior<EventCommand> onCreateWorldBoss(EventCommand.CreateWorldBoss msg) {
        Behavior<WorldBossMessage> bossBehavior = WorldBossActor.create(msg.bossId(), msg.behavior(), router, cache);
        ActorRef<WorldBossMessage> bossRef = getContext().spawn(bossBehavior, "world-boss-" + msg.bossId());
        registry.register(ActorType.WORLD_BOSS, msg.bossId(), bossRef);
        return Behaviors.same();
    }

    private Behavior<EventCommand> onCreateGuildWar(EventCommand.CreateGuildWar msg) {
        Behavior<GuildWarMessage> warBehavior = GuildWarActor.create(msg.warId(), msg.behavior(), router, cache);
        ActorRef<GuildWarMessage> warRef = getContext().spawn(warBehavior, "guild-war-" + msg.warId());
        registry.register(ActorType.GUILD_WAR, msg.warId(), warRef);
        return Behaviors.same();
    }

    private Behavior<EventCommand> onStopEvent(EventCommand.StopEvent msg) {
        registry.resolve(ActorType.WORLD_BOSS, msg.eventId()).ifPresent(ref -> getContext().stop(ref));
        registry.resolve(ActorType.GUILD_WAR, msg.eventId()).ifPresent(ref -> getContext().stop(ref));
        registry.unregister(ActorType.WORLD_BOSS, msg.eventId());
        registry.unregister(ActorType.GUILD_WAR, msg.eventId());
        return Behaviors.same();
    }
}
