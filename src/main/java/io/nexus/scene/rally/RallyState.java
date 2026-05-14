package io.nexus.actor.scene.rally;

import io.nexus.actor.view.GridId;

import java.util.*;

public class RallyState {

    public enum Phase {
        WAITING,
        MARCHING,
        BATTLING,
        RETURNING,
        DISMISSED
    }

    private final long rallyId;
    private final long initiatorId;
    private final long targetId;
    private final List<GridId> path;
    private final int maxMembers;
    private Phase phase;
    private final Map<Long, RallyMember> members = new LinkedHashMap<>();
    private long createTime;

    public RallyState(long rallyId, long initiatorId, long targetId, List<GridId> path, int maxMembers) {
        this.rallyId = rallyId;
        this.initiatorId = initiatorId;
        this.targetId = targetId;
        this.path = path;
        this.maxMembers = maxMembers;
        this.phase = Phase.WAITING;
        this.createTime = System.currentTimeMillis();
    }

    public long getRallyId() { return rallyId; }
    public long getInitiatorId() { return initiatorId; }
    public long getTargetId() { return targetId; }
    public List<GridId> getPath() { return path; }
    public int getMaxMembers() { return maxMembers; }
    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }
    public Map<Long, RallyMember> getMembers() { return members; }
    public long getCreateTime() { return createTime; }

    public boolean isFull() {
        return members.size() >= maxMembers;
    }

    public boolean hasMember(long playerId) {
        return members.containsKey(playerId);
    }

    public void addMember(long playerId, Object troops) {
        members.put(playerId, new RallyMember(playerId, troops));
    }

    public void removeMember(long playerId) {
        members.remove(playerId);
    }

    public record RallyMember(long playerId, Object troops) {}
}
