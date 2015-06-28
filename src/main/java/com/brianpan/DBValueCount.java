package com.brianpan;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DBValueCount {
  Map<String, Integer> valueCounts = new HashMap<>();

  public Set<String> getAllNames() {
    return valueCounts.keySet();
  }

  public int getCount(String value) {
    Integer count = valueCounts.get(value);
    return null == count ? 0 : count;
  }

  public void incrementCount(String value) {
    valueCounts.put(value, getCount(value) + 1);
  }

  public void decrementCount(String value) {
    int count = getCount(value);
    Preconditions.checkState(count > 0, "Attempting to decrement count when it is already zero.");
    valueCounts.put(value, count - 1);
  }
}
