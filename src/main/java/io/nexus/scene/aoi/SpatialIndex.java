package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;

import java.util.*;

/**
 * 地图对象空间索引。
 * 维护 GridId → Set<objectId> 的映射，支持快速查询某个格子/区域内的对象。
 */
public class SpatialIndex {

    private final Map<GridId, Set<Long>> gridObjects = new HashMap<>();
    private final Map<Long, GridId> objectGrid = new HashMap<>();

    /**
     * 添加对象到空间索引
     */
    public void add(long objectId, GridId gridId) {
        remove(objectId);
        gridObjects.computeIfAbsent(gridId, k -> new HashSet<>()).add(objectId);
        objectGrid.put(objectId, gridId);
    }

    /**
     * 移除对象
     */
    public GridId remove(long objectId) {
        GridId old = objectGrid.remove(objectId);
        if (old != null) {
            Set<Long> set = gridObjects.get(old);
            if (set != null) {
                set.remove(objectId);
                if (set.isEmpty()) {
                    gridObjects.remove(old);
                }
            }
        }
        return old;
    }

    /**
     * 移动对象到新格子
     * @return 旧格子（如果位置变了），null 表示没变
     */
    public GridId move(long objectId, GridId newGridId) {
        GridId old = objectGrid.get(objectId);
        if (newGridId.equals(old)) return null;
        remove(objectId);
        add(objectId, newGridId);
        return old;
    }

    /**
     * 查询某个格子内的所有对象
     */
    public Set<Long> getObjectsAt(GridId gridId) {
        return gridObjects.getOrDefault(gridId, Collections.emptySet());
    }

    /**
     * 查询多个格子内的所有对象
     */
    public Set<Long> getObjectsIn(Set<GridId> gridIds) {
        Set<Long> result = new HashSet<>();
        for (GridId gridId : gridIds) {
            result.addAll(getObjectsAt(gridId));
        }
        return result;
    }

    /**
     * 查询矩形范围内的所有对象
     */
    public Set<Long> getObjectsInRect(int minX, int minY, int maxX, int maxY) {
        Set<Long> result = new HashSet<>();
        for (int x = minX; x <= maxX; x++) {
            for (int y = minY; y <= maxY; y++) {
                result.addAll(getObjectsAt(new GridId(x, y)));
            }
        }
        return result;
    }

    /**
     * 获取对象当前所在格子
     */
    public GridId getObjectGrid(long objectId) {
        return objectGrid.get(objectId);
    }

    /**
     * 判断对象是否存在
     */
    public boolean contains(long objectId) {
        return objectGrid.containsKey(objectId);
    }

    /**
     * 获取索引中的对象总数
     */
    public int size() {
        return objectGrid.size();
    }

    /**
     * 清空索引
     */
    public void clear() {
        gridObjects.clear();
        objectGrid.clear();
    }
}
