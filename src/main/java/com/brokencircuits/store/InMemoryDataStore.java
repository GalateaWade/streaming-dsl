package com.brokencircuits.store;

import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@ToString
public class InMemoryDataStore<K, V> implements DataStore<K, V> {
  private final Map<K, V> inner = new ConcurrentHashMap<>();

  @Override
  public void put(@NotNull K key, @Nullable V value) {
    if (value == null) {
      inner.remove(key);
    } else {
      inner.put(key, value);
    }
  }

  @NotNull
  @Override
  public Optional<V> get(@NotNull K key) {
    return Optional.ofNullable(inner.get(key));
  }
}
