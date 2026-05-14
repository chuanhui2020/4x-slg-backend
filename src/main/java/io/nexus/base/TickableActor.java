package io.nexus.actor.base;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;

import java.time.Duration;

public abstract class TickableActor<T> extends AbstractBehavior<T> {

    private final TimerScheduler<T> timers;

    protected TickableActor(ActorContext<T> context, TimerScheduler<T> timers) {
        super(context);
        this.timers = timers;
    }

    protected TimerScheduler<T> getTimers() {
        return timers;
    }

    protected abstract Duration tickInterval();

    protected abstract T buildTickMessage();

    protected abstract Behavior<T> onTick();

    protected void startTick() {
        timers.startTimerWithFixedDelay("tick", buildTickMessage(), tickInterval());
    }

    protected void stopTick() {
        timers.cancel("tick");
    }

    protected void startTimer(String key, T message, Duration delay) {
        timers.startSingleTimer(key, message, delay);
    }

    protected void startTimerWithFixedDelay(String key, T message, Duration interval) {
        timers.startTimerWithFixedDelay(key, message, interval);
    }

    protected void cancelTimer(String key) {
        timers.cancel(key);
    }
}
