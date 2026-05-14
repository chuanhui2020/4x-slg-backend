package io.nexus.actor.serialize;

public interface RpcSerializer {

    byte[] serialize(Object obj);

    <T> T deserialize(byte[] data, Class<T> type);
}
