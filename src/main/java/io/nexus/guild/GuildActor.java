package io.nexus.actor.guild;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.persistence.CacheProvider;
import io.nexus.actor.persistence.PersistenceProvider;
import io.nexus.actor.router.ActorRouter;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

public class GuildActor extends AbstractBehavior<GuildMessage> {

    private final long guildId;
    private final GuildState state;
    private final GuildBehavior behavior;
    private final PersistenceProvider<Long, GuildState> persistence;
    private final CacheProvider cache;
    private final ActorRouter router;

    public static Behavior<GuildMessage> create(long guildId, GuildBehavior behavior,
                                                 PersistenceProvider<Long, GuildState> persistence,
                                                 CacheProvider cache, ActorRouter router) {
        return Behaviors.setup(ctx -> new GuildActor(ctx, guildId, behavior, persistence, cache, router));
    }

    private GuildActor(ActorContext<GuildMessage> context, long guildId, GuildBehavior behavior,
                        PersistenceProvider<Long, GuildState> persistence,
                        CacheProvider cache, ActorRouter router) {
        super(context);
        this.guildId = guildId;
        this.behavior = behavior;
        this.persistence = persistence;
        this.cache = cache;
        this.router = router;
        this.state = new GuildState(guildId);
        loadState();
    }

    @Override
    public Receive<GuildMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(GuildMessage.JoinGuild.class, this::onJoin)
                .onMessage(GuildMessage.LeaveGuild.class, this::onLeave)
                .onMessage(GuildMessage.Kick.class, this::onKick)
                .onMessage(GuildMessage.Donate.class, this::onDonate)
                .onMessage(GuildMessage.Promote.class, this::onPromote)
                .onMessage(GuildMessage.UpdateSettings.class, this::onSettings)
                .onMessage(GuildMessage.GetGuildInfo.class, this::onGetInfo)
                .onMessage(GuildMessage.Flush.class, this::onFlush)
                .build();
    }

    private Behavior<GuildMessage> onJoin(GuildMessage.JoinGuild msg) {
        if (!behavior.canJoin(state, msg.playerId())) {
            reply(msg.replyTo(), new GuildMessage.JoinResult(guildId, false, "Cannot join"));
            return Behaviors.same();
        }
        if (state.memberCount() >= behavior.maxMembers(state)) {
            reply(msg.replyTo(), new GuildMessage.JoinResult(guildId, false, "Guild is full"));
            return Behaviors.same();
        }
        behavior.onJoin(state, msg.playerId());
        persistAndCache();
        reply(msg.replyTo(), new GuildMessage.JoinResult(guildId, true, null));
        return Behaviors.same();
    }

    private Behavior<GuildMessage> onLeave(GuildMessage.LeaveGuild msg) {
        if (!state.hasMember(msg.playerId())) {
            reply(msg.replyTo(), new GuildMessage.LeaveResult(guildId, false));
            return Behaviors.same();
        }
        behavior.onLeave(state, msg.playerId());
        state.removeMember(msg.playerId());
        persistAndCache();
        reply(msg.replyTo(), new GuildMessage.LeaveResult(guildId, true));
        return Behaviors.same();
    }

    private Behavior<GuildMessage> onKick(GuildMessage.Kick msg) {
        if (!behavior.canKick(state, msg.operatorId(), msg.targetId())) {
            reply(msg.replyTo(), new GuildMessage.KickResult(guildId, false, "No permission"));
            return Behaviors.same();
        }
        state.removeMember(msg.targetId());
        persistAndCache();
        reply(msg.replyTo(), new GuildMessage.KickResult(guildId, true, null));
        return Behaviors.same();
    }

    private Behavior<GuildMessage> onDonate(GuildMessage.Donate msg) {
        if (!state.hasMember(msg.playerId())) {
            reply(msg.replyTo(), new GuildMessage.DonateResult(guildId, false));
            return Behaviors.same();
        }
        behavior.onDonate(state, msg.playerId(), msg.amount());
        persistAndCache();
        reply(msg.replyTo(), new GuildMessage.DonateResult(guildId, true));
        return Behaviors.same();
    }

    private Behavior<GuildMessage> onPromote(GuildMessage.Promote msg) {
        if (!behavior.canPromote(state, msg.operatorId(), msg.targetId(), msg.rank())) {
            reply(msg.replyTo(), new GuildMessage.PromoteResult(guildId, false, "No permission"));
            return Behaviors.same();
        }
        behavior.onPromote(state, msg.operatorId(), msg.targetId(), msg.rank());
        persistAndCache();
        reply(msg.replyTo(), new GuildMessage.PromoteResult(guildId, true, null));
        return Behaviors.same();
    }

    private Behavior<GuildMessage> onSettings(GuildMessage.UpdateSettings msg) {
        behavior.onSettingsUpdate(state, msg.operatorId(), msg.settings());
        persistAndCache();
        reply(msg.replyTo(), new GuildMessage.SettingsResult(guildId, true));
        return Behaviors.same();
    }

    private Behavior<GuildMessage> onGetInfo(GuildMessage.GetGuildInfo msg) {
        msg.replyTo().tell(new GuildMessage.GuildInfoResponse(guildId, state));
        return Behaviors.same();
    }

    private Behavior<GuildMessage> onFlush(GuildMessage.Flush msg) {
        persistence.save(guildId, state);
        msg.ack().tell(new GuildMessage.FlushComplete(guildId));
        return Behaviors.same();
    }

    private void reply(ActorAddress replyTo, Object message) {
        if (replyTo != null) {
            router.tell(replyTo, message);
        }
    }

    private void persistAndCache() {
        persistence.save(guildId, state);
    }

    private void loadState() {
        // Async load delegated to persistence provider
        persistence.load(guildId).whenComplete((loaded, ex) -> {
            if (loaded != null) {
                state.getMembers().putAll(loaded.getMembers());
                state.setName(loaded.getName());
                state.setLeaderId(loaded.getLeaderId());
                state.setLevel(loaded.getLevel());
                state.setSettings(loaded.getSettings());
            }
        });
    }
}
