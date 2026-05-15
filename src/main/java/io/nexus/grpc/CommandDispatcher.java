package io.nexus.actor.grpc;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.address.ActorType;
import io.nexus.actor.registry.ActorRegistry;
import io.nexus.actor.router.ActorRouter;
import io.nexus.actor.serialize.RpcSerializer;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private final ActorRegistry registry;
    private final ActorRouter router;
    private final RpcSerializer serializer;
    private final ActorSystem<?> system;
    private final int serverId;

    public CommandDispatcher(ActorRegistry registry, ActorRouter router,
                             RpcSerializer serializer, ActorSystem<?> system, int serverId) {
        this.registry = registry;
        this.router = router;
        this.serializer = serializer;
        this.system = system;
        this.serverId = serverId;
    }

    public CompletableFuture<CommandResult> dispatch(String commandType, long playerId,
                                                     String requestId, byte[] payload,
                                                     String sourcePodId, long deadlineMs) {
        Duration timeout = deadlineMs > 0 ? Duration.ofMillis(deadlineMs) : DEFAULT_TIMEOUT;

        return switch (commandType) {
            case "CREATE_MARCH" -> dispatchToMarchSupervisor(payload, playerId, sourcePodId, timeout);
            case "RECALL_MARCH" -> dispatchToMarchSupervisor(payload, playerId, sourcePodId, timeout);
            case "SPEED_UP_MARCH" -> dispatchToMarchSupervisor(payload, playerId, sourcePodId, timeout);
            case "JOIN_RALLY" -> dispatchToRallySupervisor(payload, playerId, sourcePodId, timeout);
            case "LEAVE_RALLY" -> dispatchToRallySupervisor(payload, playerId, sourcePodId, timeout);
            case "DEPART_RALLY" -> dispatchToRallySupervisor(payload, playerId, sourcePodId, timeout);
            case "JOIN_GUILD" -> dispatchToGuild(payload, playerId, sourcePodId, timeout);
            case "LEAVE_GUILD" -> dispatchToGuild(payload, playerId, sourcePodId, timeout);
            case "DONATE_GUILD" -> dispatchToGuild(payload, playerId, sourcePodId, timeout);
            default -> CompletableFuture.completedFuture(
                    CommandResult.failure("UNKNOWN_COMMAND", "Unknown command: " + commandType));
        };
    }

    private CompletableFuture<CommandResult> dispatchToMarchSupervisor(byte[] payload, long playerId,
                                                                       String sourcePodId, Duration timeout) {
        return askActor(ActorType.MARCH_SUPERVISOR, 0, payload, playerId, sourcePodId, timeout);
    }

    private CompletableFuture<CommandResult> dispatchToRallySupervisor(byte[] payload, long playerId,
                                                                       String sourcePodId, Duration timeout) {
        return askActor(ActorType.RALLY_SUPERVISOR, 0, payload, playerId, sourcePodId, timeout);
    }

    private CompletableFuture<CommandResult> dispatchToGuild(byte[] payload, long playerId,
                                                              String sourcePodId, Duration timeout) {
        Object command = serializer.deserialize(payload, Object.class);
        if (command instanceof GuildTargeted targeted) {
            return askActor(ActorType.GUILD, targeted.guildId(), payload, playerId, sourcePodId, timeout);
        }
        return CompletableFuture.completedFuture(
                CommandResult.failure("INVALID_PAYLOAD", "Guild command must contain guildId"));
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<CommandResult> askActor(ActorType type, long id, byte[] payload,
                                                      long playerId, String sourcePodId, Duration timeout) {
        var refOpt = registry.resolve(type, id);
        if (refOpt.isEmpty()) {
            return CompletableFuture.completedFuture(
                    CommandResult.failure("ACTOR_NOT_FOUND", type + ":" + id + " not found"));
        }

        ActorRef<Object> ref = (ActorRef<Object>) refOpt.get();
        Object message = serializer.deserialize(payload, Object.class);

        return AskPattern.<Object, Object>ask(
                ref,
                replyTo -> new CommandEnvelope(message, playerId, sourcePodId, (ActorRef<Object>) replyTo),
                timeout,
                system.scheduler()
        ).toCompletableFuture().thenApply(result -> {
            byte[] resultPayload = serializer.serialize(result);
            return CommandResult.success(resultPayload);
        }).exceptionally(ex -> {
            log.warn("Command dispatch failed for {}:{} - {}", type, id, ex.getMessage());
            return CommandResult.failure("DISPATCH_ERROR", ex.getMessage());
        });
    }

    public record CommandEnvelope(Object message, long playerId, String sourcePodId, ActorRef<Object> replyTo) {}

    public record CommandResult(boolean success, String errorCode, String errorMessage, byte[] payload) {
        public static CommandResult success(byte[] payload) {
            return new CommandResult(true, null, null, payload);
        }

        public static CommandResult failure(String errorCode, String errorMessage) {
            return new CommandResult(false, errorCode, errorMessage, null);
        }
    }

    public interface GuildTargeted {
        long guildId();
    }
}
