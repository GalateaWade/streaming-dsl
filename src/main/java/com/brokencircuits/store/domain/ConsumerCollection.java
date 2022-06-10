package com.brokencircuits.store.domain;

import com.brokencircuits.store.RegisteredConsumer;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@ToString
public class ConsumerCollection {
  private final Set<RegisteredConsumer> registeredConsumers = new HashSet<>();

  public void add(RegisteredConsumer registeredConsumer) {
    registeredConsumers.add(registeredConsumer);
  }

  public void addAll(Collection<RegisteredConsumer> consumers) {
    registeredConsumers.addAll(consumers);
  }

  public void remove(RegisteredConsumer registeredConsumer) {
    registeredConsumers.remove(registeredConsumer);
  }

  public boolean isEmpty() {
    return registeredConsumers.isEmpty();
  }

  public Set<RegisteredConsumer> all() {
    return Collections.unmodifiableSet(registeredConsumers);
  }
}
