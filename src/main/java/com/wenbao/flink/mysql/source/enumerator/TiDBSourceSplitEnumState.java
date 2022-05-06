package com.wenbao.flink.mysql.source.enumerator;

import com.wenbao.flink.mysql.source.split.TiDBSourceSplit;

import java.util.HashSet;
import java.util.Set;

public class TiDBSourceSplitEnumState {
  private final Set<TiDBSourceSplit> assignedSplits;

  public TiDBSourceSplitEnumState(Set<TiDBSourceSplit> splits) {
    this.assignedSplits = new HashSet<>(splits);
  }

  public Set<TiDBSourceSplit> assignedSplits() {
    return assignedSplits;
  }
}