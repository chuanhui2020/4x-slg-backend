package io.nexus.actor.guild;

import io.nexus.actor.address.ActorAddress;

public sealed interface GuildMessage {

    record JoinGuild(long playerId, ActorAddress replyTo) implements GuildMessage {}

    record LeaveGuild(long playerId, ActorAddress replyTo) implements GuildMessage {}

    record Kick(long operatorId, long targetId, ActorAddress replyTo) implements GuildMessage {}

    record Donate(long playerId, int amount, ActorAddress replyTo) implements GuildMessage {}

    record Promote(long operatorId, long targetId, int rank, ActorAddress replyTo) implements GuildMessage {}

    record UpdateSettings(long operatorId, Object settings, ActorAddress replyTo) implements GuildMessage {}

    record GetGuildInfo(org.apache.pekko.actor.typed.ActorRef<GuildInfoResponse> replyTo) implements GuildMessage {}

    record Flush(org.apache.pekko.actor.typed.ActorRef<FlushComplete> ack) implements GuildMessage {}

    record GuildInfoResponse(long guildId, GuildState state) implements GuildMessage {}

    record FlushComplete(long guildId) implements GuildMessage {}

    record JoinResult(long guildId, boolean success, String reason) implements GuildMessage {}

    record LeaveResult(long guildId, boolean success) implements GuildMessage {}

    record KickResult(long guildId, boolean success, String reason) implements GuildMessage {}

    record DonateResult(long guildId, boolean success) implements GuildMessage {}

    record PromoteResult(long guildId, boolean success, String reason) implements GuildMessage {}

    record SettingsResult(long guildId, boolean success) implements GuildMessage {}
}
