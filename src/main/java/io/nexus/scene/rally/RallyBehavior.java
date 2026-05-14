package io.nexus.actor.scene.rally;

import java.time.Duration;

public interface RallyBehavior {

    boolean canJoin(RallyState state, long playerId);

    void onJoin(RallyState state, long playerId, Object troops);

    void onLeave(RallyState state, long playerId);

    void onDepart(RallyState state);

    void onArrival(RallyState state);

    void onBattleResult(RallyState state, Object result);

    void onDismiss(RallyState state);

    Duration calcMarchDuration(RallyState state);

    long generateRallyId();
}
