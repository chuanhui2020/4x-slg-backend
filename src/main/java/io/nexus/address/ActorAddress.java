package io.nexus.actor.address;

public record ActorAddress(ActorType type, long id, int serverId) {

    public static ActorAddress player(long playerId, int serverId) {
        return new ActorAddress(ActorType.PLAYER, playerId, serverId);
    }

    public static ActorAddress guild(long guildId, int serverId) {
        return new ActorAddress(ActorType.GUILD, guildId, serverId);
    }

    public static ActorAddress area(int areaId, int serverId) {
        return new ActorAddress(ActorType.AREA, areaId, serverId);
    }

    public static ActorAddress marchSupervisor(int serverId) {
        return new ActorAddress(ActorType.MARCH_SUPERVISOR, 0, serverId);
    }

    public static ActorAddress rallySupervisor(int serverId) {
        return new ActorAddress(ActorType.RALLY_SUPERVISOR, 0, serverId);
    }

    public static ActorAddress event(long eventId, int serverId) {
        return new ActorAddress(ActorType.EVENT, eventId, serverId);
    }

    public static ActorAddress mapScene(int serverId) {
        return new ActorAddress(ActorType.MAP_SCENE, 0, serverId);
    }

    public static ActorAddress worldBoss(long bossId, int serverId) {
        return new ActorAddress(ActorType.WORLD_BOSS, bossId, serverId);
    }

    public static ActorAddress guildWar(long warId, int serverId) {
        return new ActorAddress(ActorType.GUILD_WAR, warId, serverId);
    }
}
