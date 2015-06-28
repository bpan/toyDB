package com.brianpan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Database {
  List<DBView> transactions = new ArrayList<>();
  // Count of values as seen in the most recent transaction
  DBValueCount valueCounts = new DBValueCount();

  public Database() {
    transactions.add(new DBView());
  }

  /**
   * SET - Add or update value
   */
  public void set(String name, String newValue) {
    String oldValue = get(name);
    if (!oldValue.equals("NULL")) {
      valueCounts.decrementCount(oldValue);
    }
    valueCounts.incrementCount(newValue);
    getCurrentTransaction().set(name, newValue);
  }

  /**
   * GET - Retrieve the value in the latest transaction
   */
  public String get(String name) {
    for (int i = transactions.size() - 1; i >= 0; i--) {
      DBView dbView = transactions.get(i);
      Object value = dbView.get(name);
      if (value instanceof Deletion) {
        return "NULL";
      } else if (null != value) {
        return value.toString();
      }
    }
    return "NULL";
  }

  /**
   * DELETE - Remove any value associated with the passed name
   */
  public void delete(String name) {
    String value = get(name);
    if (!value.equals("NULL")) {
      valueCounts.decrementCount(value);
    }
    getCurrentTransaction().set(name, new Deletion());
  }

  /**
   * COUNT - Return a count of names that have been set to the passed value
   */
  public String count(String value) {
    return String.valueOf(valueCounts.getCount(value));
  }

  /**
   * BEGIN - Create a new transaction that can be reverted
   */
  public void begin() {
    transactions.add(new DBView());
  }

  /**
   * ROLLBACK - Revert all modifications in the latest transaction
   */
  public void rollback() {
    if (transactions.size() == 1) {
      throw new NoTransactionException();
    }
    DBView revertedTransaction = getCurrentTransaction();
    transactions.remove(revertedTransaction);
    // Update value counts
    Set<String> revertedNames = revertedTransaction.getAllNames();
    for (String revertedName : revertedNames) {
      Object revertedValue = revertedTransaction.get(revertedName);
      String newValue = get(revertedName);
      if (!revertedValue.equals(newValue)) {
        if (!(revertedValue instanceof Deletion)) {
          valueCounts.decrementCount((String) revertedValue);
        }
        if (!newValue.equals("NULL")) {
          valueCounts.incrementCount(newValue);
        }
      }
    }
  }

  private DBView getCurrentTransaction() {
    return transactions.get(transactions.size() - 1);
  }

  public class NoTransactionException extends RuntimeException {
  }
}
