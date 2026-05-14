package io.nexus.actor.base;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import java.time.Duration;

public final class SupervisedActor {

    private SupervisedActor() {}

    public static <T> Behavior<T> restart(Behavior<T> behavior) {
        return Behaviors.supervise(behavior).onFailure(SupervisorStrategy.restart());
    }

    public static <T> Behavior<T> restartWithBackoff(Behavior<T> behavior,
                                                      Duration minBackoff,
                                                      Duration maxBackoff,
                                                      double randomFactor) {
        return Behaviors.supervise(behavior)
                .onFailure(SupervisorStrategy.restartWithBackoff(minBackoff, maxBackoff, randomFactor));
    }

    public static <T> Behavior<T> stop(Behavior<T> behavior) {
        return Behaviors.supervise(behavior).onFailure(SupervisorStrategy.stop());
    }

    public static <T> Behavior<T> resume(Behavior<T> behavior) {
        return Behaviors.supervise(behavior).onFailure(SupervisorStrategy.resume());
    }
}
