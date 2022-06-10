package com.brokencircuits.store.serialization;

public interface Serializer<T> {

  byte[] serialize(SerializationContext ctx, T data);
}
