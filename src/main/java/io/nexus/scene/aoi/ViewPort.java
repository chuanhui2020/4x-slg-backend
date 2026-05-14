package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;

/**
 * 视口，表示玩家当前可见的格子矩形范围。
 * 坐标系：左下角 lb（Grid 行列坐标），右上角 rt（Grid 行列坐标）。
 * 视野定义为 Grid 行列坐标，非世界像素坐标。
 */
public record ViewPort(GridId lb, GridId rt) {

    public int width() {
        return rt.x() - lb.x() + 1;
    }

    public int height() {
        return rt.y() - lb.y() + 1;
    }

    public int gridCount() {
        return width() * height();
    }

    public boolean contains(GridId grid) {
        return grid.x() >= lb.x() && grid.x() <= rt.x()
                && grid.y() >= lb.y() && grid.y() <= rt.y();
    }

    public boolean intersects(ViewPort other) {
        return lb.x() <= other.rt.x() && rt.x() >= other.lb.x()
                && lb.y() <= other.rt.y() && rt.y() >= other.lb.y();
    }

    /**
     * 裁剪到地图边界内
     */
    public ViewPort clamp(int mapWidth, int mapHeight) {
        int lbx = Math.max(0, lb.x());
        int lby = Math.max(0, lb.y());
        int rtx = Math.min(mapWidth - 1, rt.x());
        int rty = Math.min(mapHeight - 1, rt.y());
        return new ViewPort(new GridId(lbx, lby), new GridId(rtx, rty));
    }
}
