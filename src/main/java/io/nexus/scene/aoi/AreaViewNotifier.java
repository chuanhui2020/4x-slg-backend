package io.nexus.actor.scene.aoi;

import io.nexus.actor.player.PlayerMessage;
import io.nexus.actor.view.GridId;
import io.nexus.actor.view.ViewEvent;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.*;

/**
 * 区域级 AOI 视野通知器。
 * 每个 AreaActor 持有一个实例，负责：
 * 1. 管理 Grid→PlayerActor 的反转索引
 * 2. 对象变更时按 Grid 分发通知给订阅的玩家
 * 3. 行军路线进入/离开时通知视野内玩家
 */
public class AreaViewNotifier {

    private final int areaId;
    private final Map<GridId, Set<ActorRef<PlayerMessage>>> gridSubscribers = new HashMap<>();
    private final Map<ActorRef<PlayerMessage>, Set<GridId>> playerGrids = new HashMap<>();

    public AreaViewNotifier(int areaId) {
        this.areaId = areaId;
    }

    /**
     * 玩家订阅本区域内的格子
     */
    public void subscribe(ActorRef<PlayerMessage> player, Set<GridId> gridIds) {
        playerGrids.put(player, new HashSet<>(gridIds));
        for (GridId gridId : gridIds) {
            gridSubscribers.computeIfAbsent(gridId, k -> new HashSet<>()).add(player);
        }
    }

    /**
     * 玩家取消订阅本区域
     */
    public void unsubscribe(ActorRef<PlayerMessage> player) {
        Set<GridId> grids = playerGrids.remove(player);
        if (grids != null) {
            for (GridId gridId : grids) {
                Set<ActorRef<PlayerMessage>> set = gridSubscribers.get(gridId);
                if (set != null) {
                    set.remove(player);
                    if (set.isEmpty()) {
                        gridSubscribers.remove(gridId);
                    }
                }
            }
        }
    }

    /**
     * 更新玩家订阅的格子（视口移动时）
     */
    public void updateSubscription(ActorRef<PlayerMessage> player, Set<GridId> newGridIds) {
        unsubscribe(player);
        subscribe(player, newGridIds);
    }

    /**
     * 通知单个格子上的所有订阅者
     */
    public void notifyGrid(GridId gridId, ViewEvent event) {
        Set<ActorRef<PlayerMessage>> subscribers = gridSubscribers.get(gridId);
        if (subscribers != null && !subscribers.isEmpty()) {
            PlayerMessage.ViewEventMsg msg = new PlayerMessage.ViewEventMsg(event);
            for (ActorRef<PlayerMessage> subscriber : subscribers) {
                subscriber.tell(msg);
            }
        }
    }

    /**
     * 批量通知：将事件按 gridId 分发给对应的订阅者
     */
    public void notifyBatch(List<ViewEvent> events) {
        for (ViewEvent event : events) {
            notifyGrid(event.gridId(), event);
        }
    }

    /**
     * 通知所有订阅者（广播，用于区域级事件）
     */
    public void notifyAll(ViewEvent event) {
        PlayerMessage.ViewEventMsg msg = new PlayerMessage.ViewEventMsg(event);
        for (ActorRef<PlayerMessage> player : playerGrids.keySet()) {
            player.tell(msg);
        }
    }

    /**
     * 获取某个格子的订阅者数量
     */
    public int getSubscriberCount(GridId gridId) {
        Set<ActorRef<PlayerMessage>> set = gridSubscribers.get(gridId);
        return set != null ? set.size() : 0;
    }

    /**
     * 获取本区域总订阅玩家数
     */
    public int getTotalSubscribers() {
        return playerGrids.size();
    }

    /**
     * 获取某个玩家订阅的格子
     */
    public Set<GridId> getPlayerGrids(ActorRef<PlayerMessage> player) {
        return playerGrids.getOrDefault(player, Collections.emptySet());
    }

    /**
     * 判断某个格子是否有订阅者
     */
    public boolean hasSubscribers(GridId gridId) {
        Set<ActorRef<PlayerMessage>> set = gridSubscribers.get(gridId);
        return set != null && !set.isEmpty();
    }

    /**
     * 判断是否有任何玩家订阅本区域
     */
    public boolean hasAnySubscriber() {
        return !playerGrids.isEmpty();
    }

    public int getAreaId() {
        return areaId;
    }
}
