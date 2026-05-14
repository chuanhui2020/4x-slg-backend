package io.nexus.actor.router;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.address.ActorType;
import io.nexus.actor.role.ServerRoleProvider;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class ActorRouter implements ActorInvoker {

    private final LocalActorInvoker local;
    private final RemoteActorInvoker remote;
    private final ServerRoleProvider roleProvider;

    public ActorRouter(LocalActorInvoker local, RemoteActorInvoker remote, ServerRoleProvider roleProvider) {
        this.local = local;
        this.remote = remote;
        this.roleProvider = roleProvider;
    }

    @Override
    public void tell(ActorAddress target, Object message) {
        if (isLocal(target)) {
            local.tell(target, message);
        } else {
            remote.tell(target, message);
        }
    }

    @Override
    public <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout) {
        if (isLocal(target)) {
            return local.ask(target, message, timeout);
        } else {
            return remote.ask(target, message, timeout);
        }
    }

    private boolean isLocal(ActorAddress target) {
        if (target.serverId() != roleProvider.getServerId()) {
            return false;
        }
        return switch (target.type()) {
            case PLAYER -> true;
            case GUILD, AREA, MARCH_SUPERVISOR, RALLY_SUPERVISOR, EVENT, MAP_SCENE, WORLD_BOSS, GUILD_WAR ->
                    roleProvider.isMaster();
        };
    }
}
