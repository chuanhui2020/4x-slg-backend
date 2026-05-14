package io.nexus.actor.scene.event;

import io.nexus.actor.address.ActorAddress;

public sealed interface WorldBossMessage {

    record Attack(long playerId, Object troops, ActorAddress replyTo) implements WorldBossMessage {}

    record PhaseChange(int newPhase) implements WorldBossMessage {}

    record Tick() implements WorldBossMessage {}

    record GetRanking(org.apache.pekko.actor.typed.ActorRef<RankingResponse> replyTo) implements WorldBossMessage {}

    record Flush(org.apache.pekko.actor.typed.ActorRef<FlushComplete> ack) implements WorldBossMessage {}

    // Responses
    record AttackResult(long playerId, long damage, long remainingHp) implements WorldBossMessage {}

    record RankingResponse(long bossId, java.util.List<RankEntry> ranking) implements WorldBossMessage {}

    record FlushComplete(long bossId) implements WorldBossMessage {}

    record RankEntry(long playerId, long totalDamage, int rank) {}
}
