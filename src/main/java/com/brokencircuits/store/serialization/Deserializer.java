package com.brokencircuits.store.serialization;

public interface Deserializer<T> {
  T deserialize(SerializationContext ctx, byte[] data);
}
