package com.brokencircuits.store;

import com.brokencircuits.store.domain.OffsetList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LayeredDataStoreTest {


  @Test
  void put() {
    DataStore<String, OffsetList> keyStore = new InMemoryDataStore<>();
    DataStore<Long, Long> valueStore = new InMemoryDataStore<>();
    LayeredDataStore<String, Long> store = new LayeredDataStoreImpl<>(keyStore, valueStore);

    RegisteredConsumer consumer = store.registerConsumer();
    RegisteredConsumer consumer2 = store.registerConsumer();

    String testKey = "test";
    store.put(1, testKey, 25L);

    System.out.println("Contents [0]: " + store.get(0, testKey));
    System.out.println("Contents [1]: " + store.get(1, testKey));
    System.out.println("Contents [2]: " + store.get(2, testKey));
    System.out.println("Contents [3]: " + store.get(3, testKey));

    store.commitOffset(consumer, 1, testKey);

    System.out.println("");
    System.out.println("Contents [0]: " + store.get(0, testKey));
    System.out.println("Contents [1]: " + store.get(1, testKey));
    System.out.println("Contents [2]: " + store.get(2, testKey));
    System.out.println("Contents [3]: " + store.get(3, testKey));

    System.out.println("Store: " + store);

    store.commitOffset(consumer2, 1, testKey);

    System.out.println("Store: " + store);
  }


}