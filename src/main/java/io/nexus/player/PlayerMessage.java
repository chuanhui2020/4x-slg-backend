package io.nexus.actor.player;

import io.nexus.actor.address.ActorAddress;
import io.nexus.actor.view.GridId;
import io.nexus.actor.view.ViewEvent;

public sealed interface PlayerMessage {

    record SessionMsg(byte[] requestData) implements PlayerMessage {}

    record RunTask(Runnable task) implements PlayerMessage {}

    record ViewEventMsg(ViewEvent event) implements PlayerMessage {}

    record FlushView() implements PlayerMessage {}

    record OpenMap(GridId viewLB, GridId viewRT) implements PlayerMessage {}

    record CloseMap() implements PlayerMessage {}

    record MoveView(GridId viewLB, GridId viewRT) implements PlayerMessage {}

    record SceneWriteResult(Object result, ActorAddress from) implements PlayerMessage {}

    record Disconnect() implements PlayerMessage {}

    record Stop() implements PlayerMessage {}
}
