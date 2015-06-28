package com.brianpan;

import junit.framework.TestCase;

import static com.brianpan.DBCommand.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DatabaseTest extends TestCase {
  Database database;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    database = new Database();
  }

  public void testMultipleSet() throws Exception {
    database.processInput(SET, "a", "10");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("10");
    database.processInput(SET, "a", "20");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("20");
  }

  public void testSetThenDelete() throws Exception {
    database.processInput(SET, "a", "10");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("10");
    database.processInput(DELETE, "a");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("NULL");
  }

  public void testSetInNewTransaction() throws Exception {
    database.processInput(SET, "a", "10");
    database.processInput(BEGIN);
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("10");
    database.processInput(SET, "a", "20");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("20");
  }

  public void testDeleteInNewTransaction() throws Exception {
    database.processInput(SET, "a", "10");
    database.processInput(BEGIN);
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("10");
    database.processInput(DELETE, "a");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("NULL");
  }

  public void testDeleteThenAddInNewTransaction() throws Exception {
    database.processInput(SET, "a", "10");
    database.processInput(DELETE, "a");
    database.processInput(BEGIN);
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("NULL");
    database.processInput(SET, "a", "20");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("20");
  }

  public void testCountMultipleValues() {
    database.processInput(SET, "a", "10");
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("1");
    database.processInput(SET, "b", "10");
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("2");
  }

  public void testCountAfterDelete() {
    database.processInput(SET, "a", "10");
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("1");
    database.processInput(DELETE, "a");
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("0");
  }

  public void testCountAfterUpdatingValues() {
    database.processInput(SET, "a", "10");
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("1");
    assertThat(database.processInput(COUNT, "20"))
        .isEqualTo("0");
    database.processInput(SET, "a", "20");
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("0");
    assertThat(database.processInput(COUNT, "20"))
        .isEqualTo("1");
  }

  public void testEmptyRollbackGivesErrorMessage() {
    assertThat(database.processInput(ROLLBACK))
        .isEqualTo("NO TRANSACTION");
    database.processInput(BEGIN);
    assertThat(database.processInput(ROLLBACK))
        .isNull();
    assertThat(database.processInput(ROLLBACK))
        .isEqualTo("NO TRANSACTION");
  }

  public void testRollbackRevertsUpdate() {
    database.processInput(SET, "a", "10");
    database.processInput(BEGIN);
    database.processInput(SET, "a", "20");
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("20");
    database.processInput(ROLLBACK);
    assertThat(database.processInput(GET, "a"))
        .isEqualTo("10");
  }

  public void testRollbackRevertsCounts() {
    database.processInput(SET, "a", "10");
    database.processInput(SET, "b", "100");
    database.processInput(BEGIN);
    database.processInput(SET, "a", "20");
    database.processInput(DELETE, "b");
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("0");
    assertThat(database.processInput(COUNT, "20"))
        .isEqualTo("1");
    assertThat(database.processInput(COUNT, "100"))
        .isEqualTo("0");
    database.processInput(ROLLBACK);
    assertThat(database.processInput(COUNT, "10"))
        .isEqualTo("1");
    assertThat(database.processInput(COUNT, "20"))
        .isEqualTo("0");
    assertThat(database.processInput(COUNT, "100"))
        .isEqualTo("1");
  }

  public void testCommitSquashesAddsUpdatesAndDeletions() {
    database.processInput(SET, "add in base", "10");
    database.processInput(SET, "update later", "20");
    database.processInput(SET, "delete later", "30");
    database.processInput(BEGIN);
    database.processInput(SET, "update later", "40");
    database.processInput(SET, "add later", "50");
    database.processInput(DELETE, "delete later");
    database.processInput(BEGIN);
    database.processInput(SET, "add in last transaction", "60");

    assertThat(database.processInput(GET, "add in base"))
        .isEqualTo("10");
    assertThat(database.processInput(GET, "update later"))
        .isEqualTo("40");
    assertThat(database.processInput(GET, "delete later"))
        .isEqualTo("NULL");
    assertThat(database.processInput(GET, "add later"))
        .isEqualTo("50");
    assertThat(database.processInput(GET, "add in last transaction"))
        .isEqualTo("60");

    database.processInput(COMMIT);

    assertThat(database.processInput(GET, "add in base"))
        .isEqualTo("10");
    assertThat(database.processInput(GET, "update later"))
        .isEqualTo("40");
    assertThat(database.processInput(GET, "delete later"))
        .isEqualTo("NULL");
    assertThat(database.processInput(GET, "add later"))
        .isEqualTo("50");
    assertThat(database.processInput(GET, "add in last transaction"))
        .isEqualTo("60");
  }
}