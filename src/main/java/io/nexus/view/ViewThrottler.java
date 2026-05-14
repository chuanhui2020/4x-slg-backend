package io.nexus.actor.view;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ViewThrottler {

    private final int maxPendingEvents;
    private final List<ViewEvent> pendingEvents = new ArrayList<>();

    public ViewThrottler(int maxPendingEvents) {
        this.maxPendingEvents = maxPendingEvents;
    }

    public void addEvent(ViewEvent event) {
        pendingEvents.add(event);
        while (pendingEvents.size() > maxPendingEvents) {
            pendingEvents.removeFirst();
        }
    }

    public List<ViewEvent> flush() {
        if (pendingEvents.isEmpty()) {
            return Collections.emptyList();
        }
        List<ViewEvent> flushed = new ArrayList<>(pendingEvents);
        pendingEvents.clear();
        return flushed;
    }

    public boolean hasPending() {
        return !pendingEvents.isEmpty();
    }

    public int pendingCount() {
        return pendingEvents.size();
    }
}
