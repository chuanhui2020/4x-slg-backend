package io.nexus.actor.player;

import io.nexus.actor.persistence.StreamConsumer;
import io.nexus.actor.router.ActorRouter;
import io.nexus.actor.scene.aoi.AoiCalculator;
import org.apache.pekko.actor.typed.Behavior;

public interface PlayerActorFactory {

    Behavior<PlayerMessage> create(long playerId, PlayerSession session, boolean isMaster);

    default Behavior<PlayerMessage> create(long playerId, PlayerSession session, boolean isMaster,
                                            ActorRouter router, StreamConsumer streamConsumer,
                                            int serverId, PlayerMessageHandler messageHandler,
                                            AoiCalculator aoiCalculator) {
        return PlayerActor.create(playerId, session, isMaster, router, streamConsumer,
                serverId, messageHandler, aoiCalculator, 200, 100);
    }
}
