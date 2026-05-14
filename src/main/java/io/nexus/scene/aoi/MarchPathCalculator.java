package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;

import java.util.*;

/**
 * 行军路径计算器，负责行军路线与 AOI 的交互。
 */
public class MarchPathCalculator {

    private final AoiCalculator aoiCalculator;

    public MarchPathCalculator(AoiCalculator aoiCalculator) {
        this.aoiCalculator = aoiCalculator;
    }

    /**
     * 计算行军路径经过的所有区域及其格子。
     * 用于向沿途 AreaActor 注册行军路线。
     */
    public Map<Integer, Set<GridId>> calcPassingAreas(List<GridId> path) {
        return aoiCalculator.calcMarchPathGrids(path);
    }

    /**
     * 计算行军到达时间（毫秒）。
     * @param path 路径点序列
     * @param speed 速度（格/秒）
     * @return 到达时间（毫秒）
     */
    public long calcArrivalTimeMs(List<GridId> path, float speed) {
        if (speed <= 0) throw new IllegalArgumentException("Speed must be positive");
        float distance = aoiCalculator.calcPathLength(path);
        return (long) (distance / speed * 1000);
    }

    /**
     * 计算加速后的剩余到达时间。
     * @param remainingDistance 剩余距离
     * @param newSpeed 新速度
     * @return 剩余时间（毫秒）
     */
    public long calcRemainingTimeMs(float remainingDistance, float newSpeed) {
        if (newSpeed <= 0) throw new IllegalArgumentException("Speed must be positive");
        return (long) (remainingDistance / newSpeed * 1000);
    }

    /**
     * 根据出发时间和速度，计算当前行军位置。
     * @param path 路径
     * @param speed 速度（格/秒）
     * @param departTimeMs 出发时间戳
     * @param nowMs 当前时间戳
     * @return 当前位置（浮点坐标），如果已到达则返回终点
     */
    public float[] calcCurrentPosition(List<GridId> path, float speed, long departTimeMs, long nowMs) {
        if (path == null || path.isEmpty()) return new float[]{0, 0};
        if (path.size() == 1) return new float[]{path.getFirst().x(), path.getFirst().y()};

        float elapsed = (nowMs - departTimeMs) / 1000f;
        float traveled = elapsed * speed;

        float accumulated = 0;
        for (int i = 0; i < path.size() - 1; i++) {
            GridId from = path.get(i);
            GridId to = path.get(i + 1);
            float segLen = aoiCalculator.calcDistance(from, to);

            if (accumulated + segLen >= traveled) {
                float ratio = (traveled - accumulated) / segLen;
                float x = from.x() + (to.x() - from.x()) * ratio;
                float y = from.y() + (to.y() - from.y()) * ratio;
                return new float[]{x, y};
            }
            accumulated += segLen;
        }
        // 已到达终点
        GridId last = path.getLast();
        return new float[]{last.x(), last.y()};
    }

    /**
     * 计算行军在某个时刻经过的区域 ID。
     */
    public int calcCurrentAreaId(List<GridId> path, float speed, long departTimeMs, long nowMs) {
        float[] pos = calcCurrentPosition(path, speed, departTimeMs, nowMs);
        return aoiCalculator.getConfig().posToAreaId(pos[0], pos[1]);
    }

    /**
     * 反转路径（用于返程）
     */
    public List<GridId> reversePath(List<GridId> path) {
        List<GridId> reversed = new ArrayList<>(path);
        Collections.reverse(reversed);
        return reversed;
    }

    /**
     * 计算拦截点：给定行军路径和拦截位置，找到路径上最近的格子
     */
    public GridId findNearestGridOnPath(List<GridId> path, GridId interceptPoint) {
        GridId nearest = path.getFirst();
        float minDist = Float.MAX_VALUE;
        for (GridId grid : path) {
            float dist = aoiCalculator.calcDistance(grid, interceptPoint);
            if (dist < minDist) {
                minDist = dist;
                nearest = grid;
            }
        }
        return nearest;
    }
}
