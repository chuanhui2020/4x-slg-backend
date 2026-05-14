package io.nexus.actor.scene.area;

import io.nexus.actor.view.ViewEvent;

import java.util.List;

public interface AreaBehavior {

    void onTick(AreaState state, List<ViewEvent> outEvents);

    void onAttack(AttackContext ctx);

    void onObjectUpdate(MapObject obj);

    List<MapObject> generateInitialObjects(int areaId);

    record AttackContext(long marchId, long playerId, Object troops, long targetId, AreaState state) {}
}
