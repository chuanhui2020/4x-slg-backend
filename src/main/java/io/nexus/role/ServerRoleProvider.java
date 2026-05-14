package io.nexus.actor.role;

public class ServerRoleProvider {

    private final ServerRole role;
    private final int serverId;

    public ServerRoleProvider() {
        String roleEnv = System.getenv("SERVER_ROLE");
        this.role = "master".equalsIgnoreCase(roleEnv) ? ServerRole.MASTER : ServerRole.SLAVE;
        String serverIdEnv = System.getenv("SERVER_ID");
        this.serverId = serverIdEnv != null ? Integer.parseInt(serverIdEnv) : 1;
    }

    public ServerRoleProvider(ServerRole role, int serverId) {
        this.role = role;
        this.serverId = serverId;
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
}
