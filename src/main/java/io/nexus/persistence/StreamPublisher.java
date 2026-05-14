package io.nexus.actor.persistence;

public interface StreamPublisher {

    void publish(String streamKey, byte[] payload);
}
