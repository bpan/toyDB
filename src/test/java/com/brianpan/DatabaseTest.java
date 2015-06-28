package com.brianpan;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class DatabaseTest {
  Database database;

  @Before
  public void setUp() throws Exception {
    database = new Database();
  }

  @Test
  public void multipleSet() throws Exception {
    database.set("a", "10");
    assertThat(database.get("a"))
        .isEqualTo("10");
    database.set("a", "20");
    assertThat(database.get("a"))
        .isEqualTo("20");
  }

  @Test
  public void setThenDelete() throws Exception {
    database.set("a", "10");
    assertThat(database.get("a"))
        .isEqualTo("10");
    database.delete("a");
    assertThat(database.get("a"))
        .isEqualTo("NULL");
  }

  @Test
  public void setInNewTransaction() throws Exception {
    database.set("a", "10");
    database.begin();
    assertThat(database.get("a"))
        .isEqualTo("10");
    database.set("a", "20");
    assertThat(database.get("a"))
        .isEqualTo("20");
  }

  @Test
  public void deleteInNewTransaction() throws Exception {
    database.set("a", "10");
    database.begin();
    assertThat(database.get("a"))
        .isEqualTo("10");
    database.delete("a");
    assertThat(database.get("a"))
        .isEqualTo("NULL");
  }

  @Test
  public void deleteThenAddInNewTransaction() throws Exception {
    database.set("a", "10");
    database.delete("a");
    database.begin();
    assertThat(database.get("a"))
        .isEqualTo("NULL");
    database.set("a", "20");
    assertThat(database.get("a"))
        .isEqualTo("20");
  }

  @Test
  public void countMultipleValues() {
    database.set("a", "10");
    assertThat(database.count("10"))
        .isEqualTo("1");
    database.set("b", "10");
    assertThat(database.count("10"))
        .isEqualTo("2");
  }

  @Test
  public void countAfterDelete() {
    database.set("a", "10");
    assertThat(database.count("10"))
        .isEqualTo("1");
    database.delete("a");
    assertThat(database.count("10"))
        .isEqualTo("0");
  }

  @Test
  public void countAfterUpdatingValues() {
    database.set("a", "10");
    assertThat(database.count("10"))
        .isEqualTo("1");
    assertThat(database.count("20"))
        .isEqualTo("0");
    database.set("a", "20");
    assertThat(database.count("10"))
        .isEqualTo("0");
    assertThat(database.count("20"))
        .isEqualTo("1");
  }

  @Test(expected = Database.NoTransactionException.class)
  public void rollbackWithoutTransactionThrows() {
    database.rollback();
  }

  @Test(expected = Database.NoTransactionException.class)
  public void rollbackAllTransactionsPlusOneThrows() {
    database.begin();
    database.rollback();
    database.rollback();
  }

  @Test
  public void rollbackRevertsUpdate() {
    database.set("a", "10");
    database.begin();
    database.set("a", "20");
    assertThat(database.get("a"))
        .isEqualTo("20");
    database.rollback();
    assertThat(database.get("a"))
        .isEqualTo("10");
  }

  @Test
  public void rollbackRevertsCounts() {
    database.set("a", "10");
    database.set("b", "100");
    database.begin();
    database.set("a", "20");
    database.delete("b");
    assertThat(database.count("10"))
        .isEqualTo("0");
    assertThat(database.count("20"))
        .isEqualTo("1");
    assertThat(database.count("100"))
        .isEqualTo("0");
    database.rollback();
    assertThat(database.count("10"))
        .isEqualTo("1");
    assertThat(database.count("20"))
        .isEqualTo("0");
    assertThat(database.count("100"))
        .isEqualTo("1");
  }

  @Test
  public void commitSquashesAddsUpdatesAndDeletions() {
    database.set("add in base", "10");
    database.set("update later", "20");
    database.set("delete later", "30");
    database.begin();
    database.set("update later", "40");
    database.set("add later", "50");
    database.delete("delete later");
    database.begin();
    database.set("add in last transaction", "60");

    assertThat(database.get("add in base"))
        .isEqualTo("10");
    assertThat(database.get("update later"))
        .isEqualTo("40");
    assertThat(database.get("delete later"))
        .isEqualTo("NULL");
    assertThat(database.get("add later"))
        .isEqualTo("50");
    assertThat(database.get("add in last transaction"))
        .isEqualTo("60");

//    database.commit();
    fail("Commit not implemented yet.");

    assertThat(database.get("add in base"))
        .isEqualTo("10");
    assertThat(database.get("update later"))
        .isEqualTo("40");
    assertThat(database.get("delete later"))
        .isEqualTo("NULL");
    assertThat(database.get("add later"))
        .isEqualTo("50");
    assertThat(database.get("add in last transaction"))
        .isEqualTo("60");
  }
}