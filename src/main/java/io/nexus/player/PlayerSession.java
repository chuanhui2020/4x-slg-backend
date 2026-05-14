package io.nexus.actor.player;

public interface PlayerSession {

    void send(byte[] data);

    void close();

    boolean isConnected();
}
