package io.nexus.actor.scene.area;

import io.nexus.actor.view.GridId;

public interface MapObject {

    long getId();

    float getX();

    float getY();

    GridId getGridId();

    boolean isUpdatable();

    void update();

    Object snapshot();
}
