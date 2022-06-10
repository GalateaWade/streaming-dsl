package com.brokencircuits.store;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

public interface DataStore<K, V> {

  void put(@NotNull K key, @Nullable V value);

  @NotNull
  Optional<V> get(@NotNull K key);
}
