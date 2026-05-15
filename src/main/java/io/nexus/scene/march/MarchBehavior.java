package io.nexus.actor.scene.march;

import io.nexus.actor.scene.area.AreaState;
import io.nexus.actor.view.GridId;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface MarchBehavior {

    Duration calcArrivalTime(MarchState state);

    Duration recalcArrivalTime(MarchState state, float speedMultiplier);

    void onArrival(MarchState state);

    void onBattleResult(MarchState state, Object result);

    void onRecall(MarchState state);

    void onIntercept(MarchState state, long attackerId, GridId point);

    void onRecoveredArrival(MarchStateStore.MarchSnapshot snapshot);

    AreaState.MarchLineInfo toLineInfo(MarchState state);

    Function<GridId, Integer> gridToAreaMapper();

    long generateMarchId();

    boolean shouldCreateReturnMarch(MarchState state, Object battleResult);

    MarchMessage.Create buildReturnMarch(MarchState state);
}
