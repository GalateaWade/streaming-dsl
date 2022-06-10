package com.brokencircuits.store.domain;

import com.apple.foundationdb.tuple.Tuple;
import com.brokencircuits.store.serialization.Deserializer;
import com.brokencircuits.store.serialization.Serde;
import com.brokencircuits.store.serialization.Serializer;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@ToString
public class OffsetList {

  private final List<Long> offsets;

  public OffsetList(List<Long> offsets) {
    this.offsets = new ArrayList<>(offsets);
  }

  public List<Long> getOffsets() {
    return Collections.unmodifiableList(offsets);
  }

  private static final Serde<OffsetList> serde = new Serde<OffsetList>() {
    @Override
    public Serializer<OffsetList> serializer() {
      return (ctx, data) -> {
        if (data == null) {
          return null;
        }
        return Tuple.fromList(data.offsets).pack();
      };
    }

    @Override
    public Deserializer<OffsetList> deserializer() {
      return (ctx, data) -> {
        if (data == null) {
          return null;
        }
        Tuple t = Tuple.fromBytes(data);
        List<Long> offsets = t.stream().map(o -> (Long) o).collect(Collectors.toList());
        return new OffsetList(offsets);
      };
    }
  };

  public static Serde<OffsetList> serde() {
    return serde;
  }
}
