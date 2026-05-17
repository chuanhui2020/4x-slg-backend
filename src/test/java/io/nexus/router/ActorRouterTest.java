package io.nexus.actor.router;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.address.ActorType;
import io.nexus.actor.role.ServerRole;
import io.nexus.actor.role.ServerRoleProvider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ActorRouterTest {

    @Test
    void tell_playerOnSamePod_routesLocal() {
        var roleProvider = new ServerRoleProvider(ServerRole.SLAVE, 1, "slave-a");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = ActorAddress.player(100, 1, "slave-a");
        router.tell(target, "hello");

        assertEquals("local", tracker.lastRoute.get());
    }

    @Test
    void tell_playerOnDifferentPod_routesRemote() {
        var roleProvider = new ServerRoleProvider(ServerRole.SLAVE, 1, "slave-a");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = ActorAddress.player(100, 1, "slave-b");
        router.tell(target, "hello");

        assertEquals("remote", tracker.lastRoute.get());
    }

    @Test
    void tell_areaOnMaster_routesLocal() {
        var roleProvider = new ServerRoleProvider(ServerRole.MASTER, 1, "master-0");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = ActorAddress.area(5, 1);
        router.tell(target, "tick");

        assertEquals("local", tracker.lastRoute.get());
    }

    @Test
    void tell_areaOnSlave_routesRemote() {
        var roleProvider = new ServerRoleProvider(ServerRole.SLAVE, 1, "slave-a");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = ActorAddress.area(5, 1);
        router.tell(target, "tick");

        assertEquals("remote", tracker.lastRoute.get());
    }

    @Test
    void tell_differentServer_routesRemote() {
        var roleProvider = new ServerRoleProvider(ServerRole.MASTER, 1, "master-0");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = ActorAddress.area(5, 2);
        router.tell(target, "tick");

        assertEquals("remote", tracker.lastRoute.get());
    }

    @Test
    void tell_marchSupervisorOnMaster_routesLocal() {
        var roleProvider = new ServerRoleProvider(ServerRole.MASTER, 1, "master-0");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = ActorAddress.marchSupervisor(1);
        router.tell(target, "create");

        assertEquals("local", tracker.lastRoute.get());
    }

    @Test
    void tell_guildOnMaster_routesLocal() {
        var roleProvider = new ServerRoleProvider(ServerRole.MASTER, 1, "master-0");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = ActorAddress.guild(888, 1);
        router.tell(target, "join");

        assertEquals("local", tracker.lastRoute.get());
    }

    @Test
    void tell_playerWithNullPod_routesLocal() {
        var roleProvider = new ServerRoleProvider(ServerRole.SLAVE, 1, "slave-a");
        var tracker = new RoutingTracker();
        var router = new ActorRouter(tracker.local, tracker.remote, roleProvider);

        ActorAddress target = new ActorAddress(ActorType.PLAYER, 100, 1, null);
        router.tell(target, "hello");

        assertEquals("local", tracker.lastRoute.get());
    }

    private static class RoutingTracker {
        final AtomicReference<String> lastRoute = new AtomicReference<>();

        final LocalActorInvoker local = new LocalActorInvoker(null, null) {
            @Override
            public void tell(ActorAddress target, Object message) {
                lastRoute.set("local");
            }

            @Override
            public <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout) {
                lastRoute.set("local");
                return CompletableFuture.completedFuture(null);
            }
        };

        final RemoteActorInvoker remote = new RemoteActorInvoker(null, null, null) {
            @Override
            public void tell(ActorAddress target, Object message) {
                lastRoute.set("remote");
            }

            @Override
            public <T> CompletableFuture<T> ask(ActorAddress target, Object message, Duration timeout) {
                lastRoute.set("remote");
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
