package io.nexus.actor.scene.march;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.view.GridId;

import java.util.List;

public sealed interface MarchMessage {

    record Create(long playerId, List<GridId> path, float speed, long targetId, int type,
                  Object troops, ActorAddress replyTo) implements MarchMessage {}

    record ArrivalTimeout() implements MarchMessage {}

    record Recall(long playerId, ActorAddress replyTo) implements MarchMessage {}

    record Intercept(long attackerId, GridId point) implements MarchMessage {}

    record SpeedUp(float multiplier, ActorAddress replyTo) implements MarchMessage {}

    record BattleResult(Object result) implements MarchMessage {}

    // Callback messages
    record MarchCreated(long marchId) implements MarchMessage {}

    record MarchFailed(String reason) implements MarchMessage {}

    record MarchRecalled(long marchId) implements MarchMessage {}

    record MarchCompleted(long marchId) implements MarchMessage {}
}
