package io.nexus.actor.view;

import org.apache.pekko.actor.typed.ActorRef;

import java.util.*;

public class GridSubscription<T> {

    private final Map<GridId, Set<ActorRef<T>>> gridSubscribers = new HashMap<>();
    private final Map<ActorRef<T>, Set<GridId>> playerGrids = new HashMap<>();

    public void subscribe(ActorRef<T> observer, Set<GridId> gridIds) {
        playerGrids.put(observer, gridIds);
        for (GridId gridId : gridIds) {
            gridSubscribers.computeIfAbsent(gridId, k -> new HashSet<>()).add(observer);
        }
    }

    public void unsubscribe(ActorRef<T> observer) {
        Set<GridId> grids = playerGrids.remove(observer);
        if (grids != null) {
            for (GridId gridId : grids) {
                Set<ActorRef<T>> set = gridSubscribers.get(gridId);
                if (set != null) {
                    set.remove(observer);
                    if (set.isEmpty()) {
                        gridSubscribers.remove(gridId);
                    }
                }
            }
        }
    }

    public void updateSubscription(ActorRef<T> observer, Set<GridId> newGridIds) {
        unsubscribe(observer);
        subscribe(observer, newGridIds);
    }

    public Set<ActorRef<T>> getSubscribers(GridId gridId) {
        return gridSubscribers.getOrDefault(gridId, Collections.emptySet());
    }

    public Set<GridId> getSubscribedGrids(ActorRef<T> observer) {
        return playerGrids.getOrDefault(observer, Collections.emptySet());
    }

    public boolean hasSubscribers(GridId gridId) {
        Set<ActorRef<T>> set = gridSubscribers.get(gridId);
        return set != null && !set.isEmpty();
    }

    public int subscriberCount() {
        return playerGrids.size();
    }
}
