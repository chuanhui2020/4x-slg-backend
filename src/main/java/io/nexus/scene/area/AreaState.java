package io.nexus.actor.scene.area;

import io.nexus.actor.view.GridId;
import io.nexus.actor.view.ViewEvent;

import java.util.*;

public class AreaState {

    private final int areaId;
    private final Map<Long, MapObject> objects = new HashMap<>();
    private final Set<Long> dirtyObjectIds = new HashSet<>();
    private final Set<Long> removedObjectIds = new HashSet<>();
    private final Map<Long, MarchLineInfo> transientLines = new HashMap<>();
    private final Map<Long, Set<GridId>> marchLineGrids = new HashMap<>();

    public AreaState(int areaId) {
        this.areaId = areaId;
    }

    public int getAreaId() { return areaId; }

    public Map<Long, MapObject> getObjects() { return objects; }

    public void addObject(MapObject obj) {
        objects.put(obj.getId(), obj);
        dirtyObjectIds.add(obj.getId());
    }

    public MapObject removeObject(long objectId) {
        MapObject removed = objects.remove(objectId);
        if (removed != null) {
            dirtyObjectIds.remove(objectId);
            removedObjectIds.add(objectId);
        }
        return removed;
    }

    public void markDirty(long objectId) {
        dirtyObjectIds.add(objectId);
    }

    public void addMarchLine(long marchId, MarchLineInfo lineInfo, Set<GridId> grids) {
        transientLines.put(marchId, lineInfo);
        marchLineGrids.put(marchId, grids);
    }

    public void removeMarchLine(long marchId) {
        transientLines.remove(marchId);
        marchLineGrids.remove(marchId);
    }

    public Map<Long, MarchLineInfo> getTransientLines() { return transientLines; }

    public Set<GridId> getMarchLineGrids(long marchId) {
        return marchLineGrids.getOrDefault(marchId, Collections.emptySet());
    }

    public PersistenceBatch drainDirty() {
        Map<Long, MapObject> dirtyObjects = new HashMap<>();
        for (Long id : dirtyObjectIds) {
            MapObject obj = objects.get(id);
            if (obj != null) {
                dirtyObjects.put(id, obj);
            }
        }
        Set<Long> removed = new HashSet<>(removedObjectIds);
        dirtyObjectIds.clear();
        removedObjectIds.clear();
        return new PersistenceBatch(dirtyObjects, removed);
    }

    public void reMarkDirty(Set<Long> dirtyIds, Set<Long> removedIds) {
        for (long id : dirtyIds) {
            if (objects.containsKey(id)) {
                dirtyObjectIds.add(id);
            }
        }
        removedObjectIds.addAll(removedIds);
    }

    public record PersistenceBatch(Map<Long, MapObject> objects, Set<Long> removedObjectIds) {
        public boolean isEmpty() {
            return objects.isEmpty() && removedObjectIds.isEmpty();
        }
    }

    public record MarchLineInfo(long marchId, long playerId, List<GridId> path, float speed, long departTime) {}
}
