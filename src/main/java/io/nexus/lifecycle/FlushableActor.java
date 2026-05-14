package io.nexus.actor.lifecycle;

import org.apache.pekko.actor.typed.ActorRef;

public interface FlushableActor {

    record FlushComplete(String actorId) {}

    void flush(ActorRef<FlushComplete> ack);
}
