package com.brokencircuits.store;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

public interface LayeredDataStore<K,V> {

  /*
  LayeredStore get is called by logic thread
  LayeredStore put is called by consumer thread

  LayeredStore knows what offset each consumer is at
  When get is called, consumer object is passed in and correct layer is determined internally

   */
  void commitOffset(RegisteredConsumer consumer, long offset, @NotNull K key);

  RegisteredConsumer registerConsumer();

  void deRegisterConsumer(RegisteredConsumer consumer);

  void put(long offset, @NotNull K key, @Nullable V value);

  @NotNull
  Optional<V> get(long offset, @NotNull K key);
}
