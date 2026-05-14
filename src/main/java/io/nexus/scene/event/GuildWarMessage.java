package io.nexus.actor.scene.event;

import io.nexus.actor.address.ActorAddress;

public sealed interface GuildWarMessage {

    record Start(long warId, java.util.List<Long> participatingGuilds) implements GuildWarMessage {}

    record Tick() implements GuildWarMessage {}

    record CapturePoint(long playerId, long guildId, int pointId, ActorAddress replyTo) implements GuildWarMessage {}

    record PhaseChange(int newPhase) implements GuildWarMessage {}

    record End() implements GuildWarMessage {}

    record GetScore(org.apache.pekko.actor.typed.ActorRef<ScoreResponse> replyTo) implements GuildWarMessage {}

    record Flush(org.apache.pekko.actor.typed.ActorRef<FlushComplete> ack) implements GuildWarMessage {}

    // Responses
    record CaptureResult(boolean success, int pointId, long guildId) implements GuildWarMessage {}

    record ScoreResponse(long warId, java.util.Map<Long, Integer> guildScores) implements GuildWarMessage {}

    record FlushComplete(long warId) implements GuildWarMessage {}
}
