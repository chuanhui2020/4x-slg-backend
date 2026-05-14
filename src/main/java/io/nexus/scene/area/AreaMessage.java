package io.nexus.actor.scene.area;

import io.nexus.actor.player.PlayerMessage;
import io.nexus.actor.view.GridId;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.Set;

public sealed interface AreaMessage {

    record LoadComplete() implements AreaMessage {}

    record Tick() implements AreaMessage {}

    record Persist() implements AreaMessage {}

    record Flush(ActorRef<FlushComplete> ack) implements AreaMessage {}

    record FlushComplete(int areaId) implements AreaMessage {}

    record Subscribe(ActorRef<PlayerMessage> observer, Set<GridId> gridIds) implements AreaMessage {}

    record Unsubscribe(ActorRef<PlayerMessage> observer) implements AreaMessage {}

    record UpdateSubscription(ActorRef<PlayerMessage> observer, Set<GridId> newGridIds) implements AreaMessage {}

    record MarchLineEnter(long marchId, AreaState.MarchLineInfo lineInfo, Set<GridId> grids) implements AreaMessage {}

    record MarchLineLeave(long marchId) implements AreaMessage {}

    record AttackRequest(long marchId, long playerId, Object troops, long targetId,
                         io.nexus.actor.address.ActorAddress replyTo) implements AreaMessage {}

    record AddMapObject(MapObject obj, boolean notifyView) implements AreaMessage {}

    record RemoveMapObject(long objectId, boolean notifyView) implements AreaMessage {}
}
