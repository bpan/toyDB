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
    Object oldValue = getValueOrNullValue(name);
    if (!(oldValue instanceof NullValue)) {
      valueCounts.decrementCount((String) oldValue);
    }
    valueCounts.incrementCount(newValue);
    currentTransaction().set(name, newValue);
  }

  /**
   * GET - Retrieve the value in the latest transaction
   */
  public String get(String name) {
    Object value = getValueOrNullValue(name);
    return value instanceof NullValue ? "NULL" : (String) value;
  }

  /**
   * DELETE - Remove any value associated with the passed name
   */
  public void delete(String name) {
    Object value = getValueOrNullValue(name);
    if (!(value instanceof NullValue)) {
      valueCounts.decrementCount((String) value);
    }
    currentTransaction().set(name, new NullValue());
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
    DBView revertedTransaction = currentTransaction();
    transactions.remove(revertedTransaction);
    // Update value counts
    Set<String> revertedNames = revertedTransaction.getAllNames();
    for (String revertedName : revertedNames) {
      Object revertedValue = revertedTransaction.get(revertedName);
      Object newValue = getValueOrNullValue(revertedName);
      if (!revertedValue.equals(newValue)) {
        if (!(revertedValue instanceof NullValue)) {
          valueCounts.decrementCount((String) revertedValue);
        }
        if (!(newValue instanceof NullValue)) {
          valueCounts.incrementCount((String) newValue);
        }
      }
    }
  }

  /**
   * COMMIT - Commits all outstanding transactions
   */
  public void commit() {
    List<DBView> transactionsToCommit = transactions;
    transactions = new ArrayList<>();
    transactions.add(new DBView());
    valueCounts = new DBValueCount();

    for (DBView view : transactionsToCommit) {
      Set<String> names = view.getAllNames();
      for (String name : names) {
        Object value = view.get(name);
        if (value instanceof NullValue) {
          delete(name);
        } else {
          set(name, (String) value);
        }
      }
    }

    // todo bpan: purge NullValue's
  }

  private DBView currentTransaction() {
    return transactions.get(transactions.size() - 1);
  }

  private Object getValueOrNullValue(String name) {
    for (int i = transactions.size() - 1; i >= 0; i--) {
      DBView dbView = transactions.get(i);
      Object value = dbView.get(name);
      if (null != value) {
        return value;
      }
    }
    return new NullValue();
  }

  private static class NullValue {
  }

  public class NoTransactionException extends RuntimeException {

  }
}
