package com.brokencircuits.store;

import com.brokencircuits.store.domain.ConsumerCollection;
import com.brokencircuits.store.domain.ConsumersForCommit;
import com.brokencircuits.store.domain.OffsetList;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@ToString
@RequiredArgsConstructor
public class LayeredDataStoreImpl<K, V> implements LayeredDataStore<K, V> {

  private final Set<RegisteredConsumer> consumers = new HashSet<>();
  private final DataStore<K, OffsetList> keyStore;
  private final DataStore<Long, V> valueStore;
  private final Map<K, ConsumersForCommit> consumersPendingCommitByKey = new ConcurrentHashMap<>();

  /*
  Underlying store may be on disk, so we need to do point-get queries

    keyStore schema: <K,List<Offset>>
    valueStore schema: <Offset, V>

    get:
      List<Offset> offsets = keyStore.get(key)
      start at end and go backwards to find highest offset lower than provided
      valueStore.get(highestOffsetLowerThanProvided)

    put:
      valueStore.put(newRecordOffset, value);
      List<Offset> offsets = keyStore.getOrDefault(key, new LinkedList<>());
      offsets.add(newRecordOffset)
      keyStore.put(key, offsets)

    commitOffset(key, offset):
      // check if all consumers are done with this offset, if they aren't, do nothing
      List<Offset> offsets = keyStore.get(key)
      staleOffsets = find all offsets <= provided offset
      offsets.removeAll(staleOffsets)
      keyStore.put(key, offsets)
      staleOffsets.forEach(o -> valueStore.remove(o))
   */

  @Override
  public void commitOffset(RegisteredConsumer consumer, long offset, @NotNull K key) {
    Optional<ConsumersForCommit> consumersForCommit = Optional.ofNullable(consumersPendingCommitByKey.get(key));

    if (consumersForCommit.isPresent()) {
      ConsumerCollection consumers = consumersForCommit.get().allConsumersForCommit(offset);
      consumers.remove(consumer);

      if (!consumers.isEmpty()) {
        return;
      }
    }
    Optional<OffsetList> offsetList = keyStore.get(key);
    Set<Long> staleOffsets = new LinkedHashSet<>();
    Set<Long> keepOffsets = new LinkedHashSet<>();
    AtomicLong maxOffset = new AtomicLong(-1);
    offsetList.map(OffsetList::getOffsets)
        .orElse(Collections.emptyList())
        .forEach(checkOffset -> {
          maxOffset.updateAndGet(currentMax -> checkOffset > currentMax ? checkOffset : currentMax);
          if (checkOffset <= offset) {
            staleOffsets.add(checkOffset);
          } else {
            keepOffsets.add(checkOffset);
          }
        });

    staleOffsets.remove(maxOffset.get());
    keepOffsets.add(maxOffset.get());

    keyStore.put(key, new OffsetList(new ArrayList<>(keepOffsets)));
    for (Long staleOffset : staleOffsets) {
      valueStore.put(staleOffset, null);
    }
  }

  @Override
  public RegisteredConsumer registerConsumer() {
    RegisteredConsumer consumer = new RegisteredConsumer();
    consumers.add(consumer);
    return consumer;
  }

  @Override
  public void deRegisterConsumer(RegisteredConsumer consumer) {
    if (!consumers.remove(consumer)) {
      throw new IllegalStateException("Consumer was not registered");
    }
  }

  @Override
  public void put(long offset, @NotNull K key, @Nullable V value) {
//    valueStore.put(newRecordOffset, value);
//    List<Offset> offsets = keyStore.getOrDefault(key, new LinkedList<>());
//    offsets.add(newRecordOffset)
//    keyStore.put(key, offsets)

    valueStore.put(offset, value);
    List<Long> offsets = keyStore.get(key).map(OffsetList::getOffsets).map(LinkedList::new).orElseGet(LinkedList::new);
    offsets.add(offset);
    keyStore.put(key, new OffsetList(offsets));

    // all active consumers are expected to process
    ConsumersForCommit consumersForCommit = consumersPendingCommitByKey.computeIfAbsent(key, k -> new ConsumersForCommit());
    ConsumerCollection consumerCollection = consumersForCommit.consumersForCommit(offset);
    consumerCollection.addAll(consumers);
  }

  @Override
  public @NotNull Optional<V> get(long offset, @NotNull K key) {
//    List<Offset> offsets = keyStore.get(key)
//    start at end and go backwards to find highest offset lower than provided
//    valueStore.get(highestOffsetLowerThanProvided)

    List<Long> offsets = keyStore.get(key).map(OffsetList::getOffsets).orElseGet(LinkedList::new);
    if (offsets.isEmpty()) {
      return Optional.empty();
    }
    for (int i = offsets.size()-1; i >= 0; i--) {
      Long crawlerOffset = offsets.get(i);
      if (crawlerOffset < offset) {
        return valueStore.get(crawlerOffset);
      }
    }

    return Optional.empty();
  }
}
