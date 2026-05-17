package io.nexus.actor.view;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ViewThrottlerTest {

    @Test
    void addEvent_withinLimit() {
        ViewThrottler throttler = new ViewThrottler(10);
        ViewEvent event = new ViewEvent(1L, "data", ChangeType.ADDED, new GridId(0, 0));

        throttler.addEvent(event);

        assertEquals(1, throttler.pendingCount());
        assertTrue(throttler.hasPending());
    }

    @Test
    void addEvent_exceedsLimit_dropsOldest() {
        ViewThrottler throttler = new ViewThrottler(3);

        for (int i = 0; i < 5; i++) {
            throttler.addEvent(new ViewEvent(i, "data" + i, ChangeType.ADDED, new GridId(i, 0)));
        }

        assertEquals(3, throttler.pendingCount());
        List<ViewEvent> flushed = throttler.flush();
        assertEquals(2L, flushed.get(0).objectId());
        assertEquals(3L, flushed.get(1).objectId());
        assertEquals(4L, flushed.get(2).objectId());
    }

    @Test
    void flush_returnsAllAndClears() {
        ViewThrottler throttler = new ViewThrottler(100);
        throttler.addEvent(new ViewEvent(1L, "a", ChangeType.ADDED, new GridId(0, 0)));
        throttler.addEvent(new ViewEvent(2L, "b", ChangeType.CHANGED, new GridId(1, 1)));

        List<ViewEvent> flushed = throttler.flush();

        assertEquals(2, flushed.size());
        assertFalse(throttler.hasPending());
        assertEquals(0, throttler.pendingCount());
    }

    @Test
    void flush_emptyReturnsEmptyList() {
        ViewThrottler throttler = new ViewThrottler(100);

        List<ViewEvent> flushed = throttler.flush();

        assertTrue(flushed.isEmpty());
    }

    @Test
    void flush_returnedListIsIndependent() {
        ViewThrottler throttler = new ViewThrottler(100);
        throttler.addEvent(new ViewEvent(1L, "a", ChangeType.ADDED, new GridId(0, 0)));

        List<ViewEvent> flushed = throttler.flush();
        throttler.addEvent(new ViewEvent(2L, "b", ChangeType.ADDED, new GridId(1, 1)));

        assertEquals(1, flushed.size());
        assertEquals(1, throttler.pendingCount());
    }
}
