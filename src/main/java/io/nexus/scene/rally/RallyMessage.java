package io.nexus.actor.scene.rally;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.view.GridId;

import java.util.List;

public sealed interface RallyMessage {

    record Create(long initiatorId, long targetId, List<GridId> path, int maxMembers,
                  long timeoutMs, ActorAddress replyTo) implements RallyMessage {}

    record Join(long playerId, Object troops, ActorAddress replyTo) implements RallyMessage {}

    record Leave(long playerId, ActorAddress replyTo) implements RallyMessage {}

    record WaitTimeout() implements RallyMessage {}

    record Depart() implements RallyMessage {}

    record ArrivalTimeout() implements RallyMessage {}

    record BattleResult(Object result) implements RallyMessage {}

    record Dismiss() implements RallyMessage {}

    // Callback messages
    record RallyCreated(long rallyId) implements RallyMessage {}

    record JoinResult(long rallyId, boolean success, String reason) implements RallyMessage {}

    record LeaveResult(long rallyId, boolean success) implements RallyMessage {}

    record RallyDismissed(long rallyId) implements RallyMessage {}
}
