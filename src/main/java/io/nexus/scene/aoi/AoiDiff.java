package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;

import java.util.*;

/**
 * 视口移动产生的 Grid 级别订阅变更。
 *
 * @param added   新增订阅的 Grid（按 areaId 分组，用于路由到对应 AreaActor）
 * @param removed 移除订阅的 Grid（按 areaId 分组）
 * @param current 当前完整的订阅 Grid 集合（按 areaId 分组）
 */
public record AoiDiff(
        Map<Integer, Set<GridId>> added,
        Map<Integer, Set<GridId>> removed,
        Map<Integer, Set<GridId>> current
) {

    public boolean hasChanges() {
        return !added.isEmpty() || !removed.isEmpty();
    }

    /**
     * 完全新增的区域（之前没有任何 Grid 订阅的 Area）
     */
    public Set<Integer> newAreaIds() {
        Set<Integer> result = new HashSet<>(added.keySet());
        // 如果 current 中该 area 的 grid 全部来自 added，说明是全新 area
        result.removeIf(areaId -> {
            Set<GridId> currentGrids = current.get(areaId);
            Set<GridId> addedGrids = added.get(areaId);
            return currentGrids != null && !currentGrids.equals(addedGrids);
        });
        return result;
    }

    /**
     * 完全移除的区域（不再有任何 Grid 订阅的 Area）
     */
    public Set<Integer> removedAreaIds() {
        Set<Integer> result = new HashSet<>(removed.keySet());
        result.removeAll(current.keySet());
        return result;
    }

    /**
     * 获取所有新增的 Grid（不分组）
     */
    public Set<GridId> allAddedGrids() {
        Set<GridId> all = new HashSet<>();
        added.values().forEach(all::addAll);
        return all;
    }

    /**
     * 获取所有移除的 Grid（不分组）
     */
    public Set<GridId> allRemovedGrids() {
        Set<GridId> all = new HashSet<>();
        removed.values().forEach(all::addAll);
        return all;
    }
}
