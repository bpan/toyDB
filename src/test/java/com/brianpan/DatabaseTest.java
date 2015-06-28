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
}