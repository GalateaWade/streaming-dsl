package com.brokencircuits.store.domain;

import com.brokencircuits.store.RegisteredConsumer;
import lombok.ToString;

import java.util.SortedMap;
import java.util.TreeMap;

@ToString
public class ConsumersForCommit {


  private final SortedMap<Long, ConsumerCollection> consumersByCommit = new TreeMap<>();

  public ConsumerCollection allConsumersForCommit(long offset) {
    SortedMap<Long, ConsumerCollection> consumerMap = consumersByCommit.subMap(0L, offset+1);
    ConsumerCollection outputCollection = new ConsumerCollection();
    for (ConsumerCollection value : consumerMap.values()) {
      for (RegisteredConsumer consumer : value.all()) {
        outputCollection.add(consumer);
      }
    }

    return outputCollection;
  }

  public ConsumerCollection consumersForCommit(long offset) {
    return consumersByCommit.computeIfAbsent(offset, o -> new ConsumerCollection());
  }

  public void cleanup(long offset) {
    consumersByCommit.computeIfPresent(offset, (o, c) -> {
      if (c.isEmpty()) {
        return null;
      }
      return c;
    });

  }
}
