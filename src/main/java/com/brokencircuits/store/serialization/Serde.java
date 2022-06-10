package com.brokencircuits.store.serialization;

public interface Serde<T> {
  Serializer<T> serializer();
  Deserializer<T> deserializer();
}
