package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;

import java.util.*;

/**
 * AOI 管理器，维护玩家视口状态并协调 Grid 级别订阅变更。
 * 每个 PlayerActor 持有一个 AoiManager 实例。
 *
 * 订阅粒度：Grid 级别
 * Area 的作用：仅作为 Grid 的逻辑分组，用于路由到对应 AreaActor
 */
public class AoiManager {

    private final AoiCalculator calculator;
    private ViewPort currentView;
    private Map<Integer, Set<GridId>> subscribedGrids;

    public AoiManager(AoiCalculator calculator) {
        this.calculator = calculator;
        this.subscribedGrids = Collections.emptyMap();
    }

    /**
     * 打开地图，初始化 Grid 级别订阅。
     * @param lb 视口左下角 Grid 坐标
     * @param rt 视口右上角 Grid 坐标
     * @return 需要订阅的 Grid（按 areaId 分组）
     */
    public Map<Integer, Set<GridId>> openMap(GridId lb, GridId rt) {
        this.currentView = new ViewPort(lb, rt);
        this.subscribedGrids = calculator.calcSubscribedGrids(currentView);
        return subscribedGrids;
    }

    /**
     * 关闭地图，清空订阅。
     * @return 之前订阅的 Grid（按 areaId 分组，需要取消订阅）
     */
    public Map<Integer, Set<GridId>> closeMap() {
        Map<Integer, Set<GridId>> old = subscribedGrids;
        this.currentView = null;
        this.subscribedGrids = Collections.emptyMap();
        return old;
    }

    /**
     * 移动视口，计算 Grid 级别订阅变更。
     * @return AoiDiff，调用方根据 diff 发送 Subscribe/Unsubscribe 消息
     */
    public AoiDiff moveView(GridId lb, GridId rt) {
        if (currentView == null) {
            openMap(lb, rt);
            return new AoiDiff(subscribedGrids, Collections.emptyMap(), subscribedGrids);
        }
        ViewPort newView = new ViewPort(lb, rt);
        AoiDiff diff = calculator.calcViewDiff(currentView, newView);
        this.currentView = newView;
        this.subscribedGrids = diff.current();
        return diff;
    }

    /**
     * 判断某个 Grid 是否在当前视口内（精确判定）
     */
    public boolean isGridVisible(GridId grid) {
        if (currentView == null) return false;
        return currentView.contains(grid);
    }

    /**
     * 获取当前订阅的所有 Grid（按 areaId 分组）
     */
    public Map<Integer, Set<GridId>> getSubscribedGrids() {
        return subscribedGrids;
    }

    /**
     * 获取当前订阅的某个 Area 内的 Grid
     */
    public Set<GridId> getSubscribedGridsInArea(int areaId) {
        return subscribedGrids.getOrDefault(areaId, Collections.emptySet());
    }

    /**
     * 获取当前订阅涉及的所有 Area ID
     */
    public Set<Integer> getSubscribedAreaIds() {
        return subscribedGrids.keySet();
    }

    /**
     * 获取当前视口
     */
    public ViewPort getCurrentView() {
        return currentView;
    }

    /**
     * 是否已打开地图
     */
    public boolean isMapOpen() {
        return currentView != null;
    }

    public AoiCalculator getCalculator() {
        return calculator;
    }
}
