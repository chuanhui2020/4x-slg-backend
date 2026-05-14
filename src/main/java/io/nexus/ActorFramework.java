package io.nexus.actor;

import io.nexus.actor.lifecycle.GracefulShutdown;
import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.role.ServerRoleProvider;
import io.nexus.actor.router.ActorRouter;
import io.nexus.actor.router.LocalActorInvoker;
import io.nexus.actor.router.RemoteActorInvoker;
import io.nexus.actor.rpc.RpcMessageHandler;
import io.nexus.actor.rpc.RpcTransport;
import io.nexus.actor.serialize.JacksonRpcSerializer;
import io.nexus.actor.serialize.RpcSerializer;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

public class ActorFramework {

    private final ActorSystem<Void> system;
    private final ActorRegistry registry;
    private final ActorRouter router;
    private final ServerRoleProvider roleProvider;
    private final RpcSerializer serializer;
    private final RpcMessageHandler rpcMessageHandler;
    private final GracefulShutdown gracefulShutdown;

    private ActorFramework(ActorSystem<Void> system, ActorRegistry registry, ActorRouter router,
                            ServerRoleProvider roleProvider, RpcSerializer serializer,
                            RpcMessageHandler rpcMessageHandler) {
        this.system = system;
        this.registry = registry;
        this.router = router;
        this.roleProvider = roleProvider;
        this.serializer = serializer;
        this.rpcMessageHandler = rpcMessageHandler;
        this.gracefulShutdown = new GracefulShutdown(system);
    }

    public static ActorFramework create(RpcTransport rpcTransport) {
        return create(rpcTransport, new ServerRoleProvider(), new JacksonRpcSerializer());
    }

    public static ActorFramework create(RpcTransport rpcTransport, ServerRoleProvider roleProvider,
                                         RpcSerializer serializer) {
        Behavior<Void> rootBehavior = Behaviors.empty();
        ActorSystem<Void> system = ActorSystem.create(rootBehavior, "actor-framework");

        ActorRegistry registry = new ActorRegistry();
        LocalActorInvoker local = new LocalActorInvoker(registry, system);
        RemoteActorInvoker remote = new RemoteActorInvoker(rpcTransport);
        ActorRouter router = new ActorRouter(local, remote, roleProvider);
        RpcMessageHandler rpcHandler = new RpcMessageHandler(registry, serializer);

        return new ActorFramework(system, registry, router, roleProvider, serializer, rpcHandler);
    }

    public ActorSystem<Void> getSystem() { return system; }

    public ActorRegistry getRegistry() { return registry; }

    public ActorRouter getRouter() { return router; }

    public ServerRoleProvider getRoleProvider() { return roleProvider; }

    public RpcSerializer getSerializer() { return serializer; }

    public RpcMessageHandler getRpcMessageHandler() { return rpcMessageHandler; }

    public GracefulShutdown getGracefulShutdown() { return gracefulShutdown; }

    public void handleIncomingRpc(byte[] data) {
        rpcMessageHandler.handleIncoming(data);
    }

    public void shutdown() {
        gracefulShutdown.shutdown(java.time.Duration.ofSeconds(10));
    }
}
