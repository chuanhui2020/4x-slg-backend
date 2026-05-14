package io.nexus.actor.scene;

public sealed interface SceneMessage {

    record Init(int areaCount) implements SceneMessage {}

    record Shutdown(org.apache.pekko.actor.typed.ActorRef<ShutdownComplete> ack) implements SceneMessage {}

    record ShutdownComplete() implements SceneMessage {}
}
