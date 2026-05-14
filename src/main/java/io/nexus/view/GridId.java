package io.nexus.actor.view;

public record GridId(int x, int y) {

    public int toAreaId(int areaWidth, int areaHeight) {
        return (y / areaHeight) * ((int) Math.ceil(1.0)) + (x / areaWidth);
    }
}
