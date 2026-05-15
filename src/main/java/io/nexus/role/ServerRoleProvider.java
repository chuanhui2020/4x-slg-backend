package io.nexus.actor.role;

import java.util.UUID;

public class ServerRoleProvider {

    private final ServerRole role;
    private final int serverId;
    private final String podId;

    public ServerRoleProvider() {
        String roleEnv = System.getenv("SERVER_ROLE");
        this.role = "master".equalsIgnoreCase(roleEnv) ? ServerRole.MASTER : ServerRole.SLAVE;
        String serverIdEnv = System.getenv("SERVER_ID");
        this.serverId = serverIdEnv != null ? Integer.parseInt(serverIdEnv) : 1;
        String podIdEnv = System.getenv("POD_ID");
        this.podId = podIdEnv != null ? podIdEnv : UUID.randomUUID().toString().substring(0, 8);
    }

    public ServerRoleProvider(ServerRole role, int serverId, String podId) {
        this.role = role;
        this.serverId = serverId;
        this.podId = podId;
    }

    public ServerRole getRole() {
        return role;
    }

    public boolean isMaster() {
        return role == ServerRole.MASTER;
    }

    public int getServerId() {
        return serverId;
    }

    public String getPodId() {
        return podId;
    }
}
