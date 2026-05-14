package io.nexus.actor.registry;

import io.nexus.actor.address.ActorType;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ActorRegistry {

    private final ConcurrentHashMap<String, ActorRef<?>> registry = new ConcurrentHashMap<>();

    private String key(ActorType type, long id) {
        return type.name() + ":" + id;
    }

    public void register(ActorType type, long id, ActorRef<?> ref) {
        registry.put(key(type, id), ref);
    }

    public void unregister(ActorType type, long id) {
        registry.remove(key(type, id));
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<ActorRef<T>> resolve(ActorType type, long id) {
        ActorRef<?> ref = registry.get(key(type, id));
        return Optional.ofNullable((ActorRef<T>) ref);
    }

    @SuppressWarnings("unchecked")
    public <T> ActorRef<T> resolveOrThrow(ActorType type, long id) {
        ActorRef<?> ref = registry.get(key(type, id));
        if (ref == null) {
            throw new IllegalStateException("Actor not found: " + type + ":" + id);
        }
        return (ActorRef<T>) ref;
    }

    public boolean contains(ActorType type, long id) {
        return registry.containsKey(key(type, id));
    }

    public int size() {
        return registry.size();
    }
}
