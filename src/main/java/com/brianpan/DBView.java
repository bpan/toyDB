package com.brianpan;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DBView {
  Map<String, Object> view = new HashMap<>();

  public void set(String name, Object value) {
    view.put(name, value);
  }

  public Object get(String name) {
    return view.get(name);
  }

  public Set<String> getAllNames() {
    return view.keySet();
  }
}
