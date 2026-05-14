package io.nexus.actor.view;

public record ViewEvent(long objectId, Object snapshot, ChangeType changeType, GridId gridId, int areaId) {

    public ViewEvent(long objectId, Object snapshot, ChangeType changeType, GridId gridId) {
        this(objectId, snapshot, changeType, gridId, 0);
    }
}
