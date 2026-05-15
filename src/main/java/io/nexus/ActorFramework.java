package io.nexus.actor;

import io.nexus.actor.grpc.CommandDispatcher;
import io.nexus.actor.grpc.MasterGrpcClient;
import io.nexus.actor.grpc.MasterGrpcServer;
import io.nexus.actor.lifecycle.GracefulShutdown;
import io.nexus.actor.redis.ReliableEventConsumer;
import io.nexus.actor.redis.ReliableEventPublisher;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ActorFramework {

    private static final Logger log = LoggerFactory.getLogger(ActorFramework.class);
    private static final int DEFAULT_GRPC_PORT = 9090;

    private final ActorSystem<Void> system;
    private final ActorRegistry registry;
    private final ActorRouter router;
    private final ServerRoleProvider roleProvider;
    private final RpcSerializer serializer;
    private final RpcMessageHandler rpcMessageHandler;
    private final GracefulShutdown gracefulShutdown;

    // Master components
    private MasterGrpcServer grpcServer;
    private ReliableEventPublisher reliableEventPublisher;

    // Slave components
    private MasterGrpcClient grpcClient;
    private ReliableEventConsumer reliableEventConsumer;

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

        MasterGrpcClient grpcClient = null;
        if (!roleProvider.isMaster()) {
            String masterAddr = System.getenv("MASTER_GRPC_ADDR");
            if (masterAddr == null) masterAddr = "game-master:9090";
            grpcClient = new MasterGrpcClient(masterAddr, roleProvider.getServerId(), roleProvider.getPodId());
        }

        RemoteActorInvoker remote = new RemoteActorInvoker(rpcTransport, grpcClient, serializer);
        ActorRouter router = new ActorRouter(local, remote, roleProvider);
        RpcMessageHandler rpcHandler = new RpcMessageHandler(registry, serializer);

        ActorFramework framework = new ActorFramework(system, registry, router, roleProvider, serializer, rpcHandler);
        framework.grpcClient = grpcClient;
        return framework;
    }

    /**
     * Master: 启动 gRPC Server + CommandDispatcher
     */
    public void startGrpcServer() throws IOException {
        if (!roleProvider.isMaster()) {
            log.warn("startGrpcServer called on non-master pod, skipping");
            return;
        }
        String portEnv = System.getenv("GRPC_PORT");
        int port = portEnv != null ? Integer.parseInt(portEnv) : DEFAULT_GRPC_PORT;

        CommandDispatcher dispatcher = new CommandDispatcher(
                registry, router, serializer, system, roleProvider.getServerId());
        grpcServer = new MasterGrpcServer(port, dispatcher);
        grpcServer.start();
    }

    public void setReliableEventPublisher(ReliableEventPublisher publisher) {
        this.reliableEventPublisher = publisher;
    }

    public void setReliableEventConsumer(ReliableEventConsumer consumer) {
        this.reliableEventConsumer = consumer;
        consumer.start();
    }

    public ActorSystem<Void> getSystem() { return system; }

    public ActorRegistry getRegistry() { return registry; }

    public ActorRouter getRouter() { return router; }

    public ServerRoleProvider getRoleProvider() { return roleProvider; }

    public RpcSerializer getSerializer() { return serializer; }

    public RpcMessageHandler getRpcMessageHandler() { return rpcMessageHandler; }

    public GracefulShutdown getGracefulShutdown() { return gracefulShutdown; }

    public ReliableEventPublisher getReliableEventPublisher() { return reliableEventPublisher; }

    public MasterGrpcClient getGrpcClient() { return grpcClient; }

    public void shutdown() {
        if (grpcServer != null) grpcServer.stop();
        if (grpcClient != null) grpcClient.shutdown();
        if (reliableEventConsumer != null) reliableEventConsumer.stop();
        gracefulShutdown.shutdown(java.time.Duration.ofSeconds(10));
    }
}
