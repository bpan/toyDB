package com.brianpan;

import java.util.HashMap;
import java.util.Map;

public class DBView {
  Map<String, Object> view = new HashMap<>();

  public void set(String name, Object value) {
    view.put(name, value);
  }

  public Object get(String name) {
    return view.get(name);
  }
}
