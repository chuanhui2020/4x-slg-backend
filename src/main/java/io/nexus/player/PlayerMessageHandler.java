package io.nexus.actor.player;

import io.nexus.actor.view.ViewEvent;

import java.util.List;

public interface PlayerMessageHandler {

    void handleRequest(long playerId, byte[] requestData, PlayerSession session);

    void handleSceneResult(long playerId, Object result, PlayerSession session);

    byte[] buildViewUpdate(List<ViewEvent> events);

    List<ViewEvent> deserializeViewEvents(byte[] data);
}
