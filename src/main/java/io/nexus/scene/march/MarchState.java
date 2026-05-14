package io.nexus.actor.scene.march;

import io.nexus.actor.view.GridId;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MarchState {

    private final long marchId;
    private final long playerId;
    private final List<GridId> path;
    private final float speed;
    private final long targetId;
    private final int type;
    private final Object troops;
    private final long departTime;

    public MarchState(long marchId, long playerId, List<GridId> path, float speed,
                      long targetId, int type, Object troops) {
        this.marchId = marchId;
        this.playerId = playerId;
        this.path = path;
        this.speed = speed;
        this.targetId = targetId;
        this.type = type;
        this.troops = troops;
        this.departTime = System.currentTimeMillis();
    }

    public long getMarchId() { return marchId; }
    public long getPlayerId() { return playerId; }
    public List<GridId> getPath() { return path; }
    public float getSpeed() { return speed; }
    public long getTargetId() { return targetId; }
    public int getType() { return type; }
    public Object getTroops() { return troops; }
    public long getDepartTime() { return departTime; }

    public GridId getStartPoint() { return path.getFirst(); }
    public GridId getEndPoint() { return path.getLast(); }

    public Map<Integer, Set<GridId>> calcAreaGrids(java.util.function.Function<GridId, Integer> gridToArea) {
        Map<Integer, Set<GridId>> result = new java.util.HashMap<>();
        for (GridId grid : path) {
            int areaId = gridToArea.apply(grid);
            result.computeIfAbsent(areaId, k -> new java.util.HashSet<>()).add(grid);
        }
        return result;
    }
}
