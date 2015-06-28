package com.brianpan;

import com.google.common.base.Preconditions;

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

  public String processInput(DBCommand command, String... args) {
    DBView currentTransaction = transactions.get(transactions.size() - 1);
    switch (command) {
      case SET:
        Preconditions.checkArgument(args.length == 2, "Invalid number of arguments for " + command);
        String name = args[0];
        String oldValue = getValueInTransaction(name);
        if (!oldValue.equals("NULL")) {
          valueCounts.decrementCount(oldValue);
        }
        String newValue = args[1];
        valueCounts.incrementCount(newValue);
        currentTransaction.set(name, newValue);
        return null;
      case GET:
        Preconditions.checkArgument(args.length == 1, "Invalid number of arguments for " + command);
        return getValueInTransaction(args[0]);
      case DELETE:
        Preconditions.checkArgument(args.length == 1, "Invalid number of arguments for " + command);
        String value = getValueInTransaction(args[0]);
        if (!value.equals("NULL")) {
          valueCounts.decrementCount(value);
        }
        currentTransaction.set(args[0], new Deletion());
        return null;
      case COUNT:
        Preconditions.checkArgument(args.length == 1, "Invalid number of arguments for " + command);
        return String.valueOf(valueCounts.getCount(args[0]));
      case END:
        return null;
      case BEGIN:
        transactions.add(new DBView());
        return null;
      case ROLLBACK:
        if (transactions.size() == 1) {
          return "NO TRANSACTION";
        }
        transactions.remove(currentTransaction);
        // Update value counts
        Set<String> transactionNames = currentTransaction.getAllNames();
        for (String transactionName : transactionNames) {
          Object revertedValue = currentTransaction.get(transactionName);
          newValue = getValueInTransaction(transactionName);
          if (!revertedValue.equals(newValue)) {
            if (!(revertedValue instanceof Deletion)) {
              valueCounts.decrementCount((String) revertedValue);
            }
            if (!newValue.equals("NULL")) {
              valueCounts.incrementCount(newValue);
            }
          }
        }
        return null;
      case COMMIT:
        // todo bpan
        throw new UnsupportedOperationException("Not yet implemented.");
      default:
        throw new UnsupportedOperationException("Unsupported command " + command);
    }
  }

  private String getValueInTransaction(String name) {
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
}
