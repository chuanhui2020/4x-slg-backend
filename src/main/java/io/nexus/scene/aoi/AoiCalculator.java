package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;

import java.util.*;

/**
 * AOI 计算器，纯 Grid 级别订阅。
 *
 * 核心模型：
 * - 视口 = GridId 矩形 (viewLB, viewRT)
 * - 订阅粒度 = Grid
 * - Area 仅作为 Grid 的逻辑分组（路由到哪个 AreaActor），不参与 AOI 判定
 * - 广播判定：对象所在 Grid → 该 Grid 的 Observer → 推送
 */
public class AoiCalculator {

    private final WorldMapConfig config;

    public AoiCalculator(WorldMapConfig config) {
        this.config = config;
    }

    /**
     * 根据视口计算需要订阅的 Grid，按 areaId 分组（用于路由）。
     */
    public Map<Integer, Set<GridId>> calcSubscribedGrids(ViewPort viewPort) {
        ViewPort clamped = viewPort.clamp(config.getMapWidth(), config.getMapHeight());
        Map<Integer, Set<GridId>> result = new HashMap<>();

        for (int x = clamped.lb().x(); x <= clamped.rt().x(); x++) {
            for (int y = clamped.lb().y(); y <= clamped.rt().y(); y++) {
                int areaId = config.gridToAreaId(x, y);
                result.computeIfAbsent(areaId, k -> new HashSet<>()).add(new GridId(x, y));
            }
        }
        return result;
    }

    /**
     * 计算视口移动后的 Grid 级别订阅变更。
     */
    public AoiDiff calcViewDiff(ViewPort oldView, ViewPort newView) {
        Map<Integer, Set<GridId>> oldGrids = calcSubscribedGrids(oldView);
        Map<Integer, Set<GridId>> newGrids = calcSubscribedGrids(newView);

        Map<Integer, Set<GridId>> added = new HashMap<>();
        Map<Integer, Set<GridId>> removed = new HashMap<>();

        // 新增的 Grid
        for (var entry : newGrids.entrySet()) {
            int areaId = entry.getKey();
            Set<GridId> newSet = entry.getValue();
            Set<GridId> oldSet = oldGrids.getOrDefault(areaId, Collections.emptySet());
            Set<GridId> addedSet = new HashSet<>(newSet);
            addedSet.removeAll(oldSet);
            if (!addedSet.isEmpty()) {
                added.put(areaId, addedSet);
            }
        }

        // 移除的 Grid
        for (var entry : oldGrids.entrySet()) {
            int areaId = entry.getKey();
            Set<GridId> oldSet = entry.getValue();
            Set<GridId> newSet = newGrids.getOrDefault(areaId, Collections.emptySet());
            Set<GridId> removedSet = new HashSet<>(oldSet);
            removedSet.removeAll(newSet);
            if (!removedSet.isEmpty()) {
                removed.put(areaId, removedSet);
            }
        }

        return new AoiDiff(added, removed, newGrids);
    }

    /**
     * 计算行军路径经过的 Grid，按 areaId 分组。
     * 使用 Bresenham 直线算法插值路径点之间的格子。
     */
    public Map<Integer, Set<GridId>> calcMarchPathGrids(List<GridId> path) {
        Map<Integer, Set<GridId>> result = new HashMap<>();
        if (path == null || path.isEmpty()) return result;

        if (path.size() == 1) {
            GridId p = path.getFirst();
            int areaId = config.gridToAreaId(p.x(), p.y());
            result.computeIfAbsent(areaId, k -> new HashSet<>()).add(p);
            return result;
        }

        for (int i = 0; i < path.size() - 1; i++) {
            GridId from = path.get(i);
            GridId to = path.get(i + 1);
            List<GridId> lineGrids = bresenhamLine(from.x(), from.y(), to.x(), to.y());
            for (GridId grid : lineGrids) {
                int areaId = config.gridToAreaId(grid.x(), grid.y());
                result.computeIfAbsent(areaId, k -> new HashSet<>()).add(grid);
            }
        }
        return result;
    }

    /**
     * 计算两点之间的直线距离（格子单位）
     */
    public float calcDistance(GridId from, GridId to) {
        float dx = to.x() - from.x();
        float dy = to.y() - from.y();
        return (float) Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * 计算路径总长度
     */
    public float calcPathLength(List<GridId> path) {
        if (path == null || path.size() < 2) return 0;
        float total = 0;
        for (int i = 0; i < path.size() - 1; i++) {
            total += calcDistance(path.get(i), path.get(i + 1));
        }
        return total;
    }

    /**
     * 获取某个格子周围指定半径内的所有格子
     */
    public Set<GridId> getGridsInRadius(GridId center, int radius) {
        Set<GridId> grids = new HashSet<>();
        int minX = Math.max(0, center.x() - radius);
        int maxX = Math.min(config.getMapWidth() - 1, center.x() + radius);
        int minY = Math.max(0, center.y() - radius);
        int maxY = Math.min(config.getMapHeight() - 1, center.y() + radius);

        int radiusSq = radius * radius;
        for (int x = minX; x <= maxX; x++) {
            for (int y = minY; y <= maxY; y++) {
                int dx = x - center.x();
                int dy = y - center.y();
                if (dx * dx + dy * dy <= radiusSq) {
                    grids.add(new GridId(x, y));
                }
            }
        }
        return grids;
    }

    /**
     * 获取格子所属的区域 ID
     */
    public int getAreaId(GridId grid) {
        return config.gridToAreaId(grid.x(), grid.y());
    }

    public WorldMapConfig getConfig() {
        return config;
    }

    /**
     * Bresenham 直线算法
     */
    private List<GridId> bresenhamLine(int x0, int y0, int x1, int y1) {
        List<GridId> grids = new ArrayList<>();
        int dx = Math.abs(x1 - x0);
        int dy = Math.abs(y1 - y0);
        int sx = x0 < x1 ? 1 : -1;
        int sy = y0 < y1 ? 1 : -1;
        int err = dx - dy;

        while (true) {
            if (x0 >= 0 && x0 < config.getMapWidth() && y0 >= 0 && y0 < config.getMapHeight()) {
                grids.add(new GridId(x0, y0));
            }
            if (x0 == x1 && y0 == y1) break;
            int e2 = 2 * err;
            if (e2 > -dy) {
                err -= dy;
                x0 += sx;
            }
            if (e2 < dx) {
                err += dx;
                y0 += sy;
            }
        }
        return grids;
    }
}
