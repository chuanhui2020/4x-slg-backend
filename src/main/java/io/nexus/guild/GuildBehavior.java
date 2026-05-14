package io.nexus.actor.guild;

public interface GuildBehavior {

    boolean canJoin(GuildState state, long playerId);

    boolean canKick(GuildState state, long operatorId, long targetId);

    boolean canPromote(GuildState state, long operatorId, long targetId, int rank);

    void onJoin(GuildState state, long playerId);

    void onLeave(GuildState state, long playerId);

    void onDonate(GuildState state, long playerId, int amount);

    void onPromote(GuildState state, long operatorId, long targetId, int rank);

    void onSettingsUpdate(GuildState state, long operatorId, Object settings);

    int maxMembers(GuildState state);
}
