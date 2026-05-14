package io.nexus.actor.guild;

import java.util.*;

public class GuildState {

    private long guildId;
    private String name;
    private long leaderId;
    private int level;
    private final Map<Long, GuildMember> members = new HashMap<>();
    private final List<Long> applyList = new ArrayList<>();
    private Object settings;

    public GuildState(long guildId) {
        this.guildId = guildId;
    }

    public long getGuildId() { return guildId; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public long getLeaderId() { return leaderId; }
    public void setLeaderId(long leaderId) { this.leaderId = leaderId; }
    public int getLevel() { return level; }
    public void setLevel(int level) { this.level = level; }
    public Object getSettings() { return settings; }
    public void setSettings(Object settings) { this.settings = settings; }

    public Map<Long, GuildMember> getMembers() { return members; }
    public List<Long> getApplyList() { return applyList; }

    public boolean hasMember(long playerId) {
        return members.containsKey(playerId);
    }

    public int memberCount() {
        return members.size();
    }

    public void addMember(GuildMember member) {
        members.put(member.playerId(), member);
    }

    public void removeMember(long playerId) {
        members.remove(playerId);
    }

    public record GuildMember(long playerId, int rank, long joinTime) {}
}
