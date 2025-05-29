package com.google.edwmigration.validation.application.validator;

import com.google.common.collect.ImmutableMap;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author nehanene */
@RunWith(JUnit4.class)
public class ValidationColumnMappingTest {

  // --- Tests for ColumnEntry inner class ---

  @Test
  public void testColumnEntry_AllNonNullNamesAndDataTypes() {
    ValidationColumnMapping.ColumnEntry entry = new ValidationColumnMapping.ColumnEntry(
        "source_col",
        "target_col",
        SQLDataType.VARCHAR,
        SQLDataType.VARCHAR,
        true
    );

    assertEquals("source_col", entry.getSourceColumnName());
    assertEquals("target_col", entry.getTargetColumnName());
    assertEquals(SQLDataType.VARCHAR, entry.getSourceColumnDataType());
    assertEquals(SQLDataType.VARCHAR, entry.getTargetColumnDataType());
    assertTrue(entry.isPrimaryKey());
    assertEquals("s_source_col", entry.getSourceColumnAlias());
    assertEquals("t_target_col", entry.getTargetColumnAlias());
  }

  @Test
  public void testColumnEntry_NullSourceColumnName() {
    ValidationColumnMapping.ColumnEntry entry = new ValidationColumnMapping.ColumnEntry(
        null,
        "target_col",
        null, // Source data type can be null if name is null
        SQLDataType.INTEGER,
        false
    );

    assertNull(entry.getSourceColumnName());
    assertEquals("target_col", entry.getTargetColumnName());
    assertNull(entry.getSourceColumnDataType());
    assertEquals(SQLDataType.INTEGER, entry.getTargetColumnDataType());
    assertFalse(entry.isPrimaryKey());
    assertNull(entry.getSourceColumnAlias()); // Alias should be null
    assertEquals("t_target_col", entry.getTargetColumnAlias());
  }

  @Test
  public void testColumnEntry_NullTargetColumnName() {
    ValidationColumnMapping.ColumnEntry entry = new ValidationColumnMapping.ColumnEntry(
        "source_col",
        null,
        SQLDataType.BIGINT,
        null, // Target data type can be null if name is null
        true
    );

    assertEquals("source_col", entry.getSourceColumnName());
    assertNull(entry.getTargetColumnName());
    assertEquals(SQLDataType.BIGINT, entry.getSourceColumnDataType());
    assertNull(entry.getTargetColumnDataType());
    assertTrue(entry.isPrimaryKey());
    assertEquals("s_source_col", entry.getSourceColumnAlias());
    assertNull(entry.getTargetColumnAlias()); // Alias should be null
  }

  @Test
  public void testColumnEntry_AliasesAreGeneratedCorrectly() {
    ValidationColumnMapping.ColumnEntry entry = new ValidationColumnMapping.ColumnEntry(
        "column_with_spaces",
        "another_column",
        SQLDataType.VARCHAR,
        SQLDataType.VARCHAR,
        false
    );
    assertEquals("s_column_with_spaces", entry.getSourceColumnAlias());
    assertEquals("t_another_column", entry.getTargetColumnAlias());
  }


  // --- Tests for ValidationColumnMapping class and buildColumnEntries() ---

  @Test
  public void testValidationColumnMapping_DirectMatchingColumns() {
    // Source columns
    HashMap<String, DataType<?>> sourceCols = new HashMap<>();
    sourceCols.put("id", SQLDataType.INTEGER);
    sourceCols.put("name", SQLDataType.VARCHAR);

    // Target columns
    HashMap<String, DataType<?>> targetCols = new HashMap<>();
    targetCols.put("id", SQLDataType.INTEGER);
    targetCols.put("name", SQLDataType.VARCHAR);

    // No explicit column mappings or primary keys
    ImmutableMap<String, String> columnMappings = ImmutableMap.of();
    ImmutableMap<String, String> primaryKeys = ImmutableMap.of();

    ValidationColumnMapping mapping = new ValidationColumnMapping(columnMappings, sourceCols, targetCols, primaryKeys);
    List<ValidationColumnMapping.ColumnEntry> entries = mapping.getColumnEntries();

    assertNotNull(entries);
    assertEquals(2, entries.size());

    ValidationColumnMapping.ColumnEntry idEntry = entries.stream()
        .filter(e -> "id".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("ID column entry not found"));
    assertEquals("id", idEntry.getSourceColumnName());
    assertEquals("id", idEntry.getTargetColumnName());
    assertEquals(SQLDataType.INTEGER, idEntry.getSourceColumnDataType());
    assertEquals(SQLDataType.INTEGER, idEntry.getTargetColumnDataType());
    assertFalse(idEntry.isPrimaryKey());

    ValidationColumnMapping.ColumnEntry nameEntry = entries.stream()
        .filter(e -> "name".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("Name column entry not found"));
    assertEquals("name", nameEntry.getSourceColumnName());
    assertEquals("name", nameEntry.getTargetColumnName());
    assertEquals(SQLDataType.VARCHAR, nameEntry.getSourceColumnDataType());
    assertEquals(SQLDataType.VARCHAR, nameEntry.getTargetColumnDataType());
    assertFalse(nameEntry.isPrimaryKey());
  }

  @Test
  public void testValidationColumnMapping_WithColumnMappings() {
    // Source columns
    HashMap<String, DataType<?>> sourceCols = new HashMap<>();
    sourceCols.put("s_id", SQLDataType.INTEGER);
    sourceCols.put("s_name", SQLDataType.VARCHAR);

    // Target columns
    HashMap<String, DataType<?>> targetCols = new HashMap<>();
    targetCols.put("t_id", SQLDataType.INTEGER);
    targetCols.put("t_full_name", SQLDataType.VARCHAR);

    // Explicit column mappings
    ImmutableMap<String, String> columnMappings = ImmutableMap.of(
        "s_id", "t_id",
        "s_name", "t_full_name"
    );
    ImmutableMap<String, String> primaryKeys = ImmutableMap.of(); // No PKs

    ValidationColumnMapping mapping = new ValidationColumnMapping(columnMappings, sourceCols, targetCols, primaryKeys);
    List<ValidationColumnMapping.ColumnEntry> entries = mapping.getColumnEntries();

    assertEquals(2, entries.size());

    ValidationColumnMapping.ColumnEntry idEntry = entries.stream()
        .filter(e -> "s_id".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("s_id column entry not found"));
    assertEquals("s_id", idEntry.getSourceColumnName());
    assertEquals("t_id", idEntry.getTargetColumnName());
    assertEquals(SQLDataType.INTEGER, idEntry.getSourceColumnDataType());
    assertEquals(SQLDataType.INTEGER, idEntry.getTargetColumnDataType());

    ValidationColumnMapping.ColumnEntry nameEntry = entries.stream()
        .filter(e -> "s_name".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("s_name column entry not found"));
    assertEquals("s_name", nameEntry.getSourceColumnName());
    assertEquals("t_full_name", nameEntry.getTargetColumnName());
    assertEquals(SQLDataType.VARCHAR, nameEntry.getSourceColumnDataType());
    assertEquals(SQLDataType.VARCHAR, nameEntry.getTargetColumnDataType());
  }

  @Test
  public void testValidationColumnMapping_WithPrimaryKeys() {
    // Source columns
    HashMap<String, DataType<?>> sourceCols = new HashMap<>();
    sourceCols.put("product_id", SQLDataType.INTEGER);
    sourceCols.put("product_name", SQLDataType.VARCHAR);

    // Target columns (same names)
    HashMap<String, DataType<?>> targetCols = new HashMap<>();
    targetCols.put("product_id", SQLDataType.INTEGER);
    targetCols.put("product_name", SQLDataType.VARCHAR);

    // Primary keys
    ImmutableMap<String, String> primaryKeys = ImmutableMap.of("product_id", "product_id");

    ValidationColumnMapping mapping = new ValidationColumnMapping(ImmutableMap.of(), sourceCols, targetCols, primaryKeys);
    List<ValidationColumnMapping.ColumnEntry> entries = mapping.getColumnEntries();

    assertEquals(2, entries.size());

    ValidationColumnMapping.ColumnEntry idEntry = entries.stream()
        .filter(e -> "product_id".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("product_id column entry not found"));
    assertTrue(idEntry.isPrimaryKey());
    assertEquals("product_id", idEntry.getTargetColumnName()); // Should match target name

    ValidationColumnMapping.ColumnEntry nameEntry = entries.stream()
        .filter(e -> "product_name".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("product_name column entry not found"));
    assertFalse(nameEntry.isPrimaryKey());
  }

  @Test(expected = RuntimeException.class)
  public void testValidationColumnMapping_PrimaryKeyMappingConflict() {

    HashMap<String, DataType<?>> sourceCols = new HashMap<>();
    sourceCols.put("pk_col", SQLDataType.INTEGER);

    HashMap<String, DataType<?>> targetCols = new HashMap<>();
    targetCols.put("other_pk_col", SQLDataType.INTEGER);
    targetCols.put("pk_col", SQLDataType.INTEGER);

    ImmutableMap<String, String> columnMappings = ImmutableMap.of("pk_col", "other_pk_col");
    ImmutableMap<String, String> primaryKeys = ImmutableMap.of("pk_col", "pk_col");

    new ValidationColumnMapping(columnMappings, sourceCols, targetCols, primaryKeys);
  }

  @Test(expected = RuntimeException.class)
  public void testValidationColumnMapping_DataTypeMismatch() {
    HashMap<String, DataType<?>> sourceCols = new HashMap<>();
    sourceCols.put("id", SQLDataType.INTEGER);

    HashMap<String, DataType<?>> targetCols = new HashMap<>();
    targetCols.put("id", SQLDataType.VARCHAR);

    new ValidationColumnMapping(ImmutableMap.of(), sourceCols, targetCols, ImmutableMap.of());
  }

  @Test
  public void testValidationColumnMapping_SourceColumnNoTargetMatch() {
    HashMap<String, DataType<?>> sourceCols = new HashMap<>();
    sourceCols.put("id", SQLDataType.INTEGER);
    sourceCols.put("name", SQLDataType.VARCHAR);

    HashMap<String, DataType<?>> targetCols = new HashMap<>();
    targetCols.put("id", SQLDataType.INTEGER);

    ValidationColumnMapping mapping = new ValidationColumnMapping(ImmutableMap.of(), sourceCols, targetCols, ImmutableMap.of());
    List<ValidationColumnMapping.ColumnEntry> entries = mapping.getColumnEntries();

    assertEquals(2, entries.size());

    ValidationColumnMapping.ColumnEntry idEntry = entries.stream()
        .filter(e -> "id".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("ID column entry not found"));
    assertNotNull(idEntry.getTargetColumnName());

    ValidationColumnMapping.ColumnEntry nameEntry = entries.stream()
        .filter(e -> "name".equals(e.getSourceColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("Name column entry not found"));
    assertNull(nameEntry.getTargetColumnName());
    assertNull(nameEntry.getTargetColumnDataType());
    assertFalse(nameEntry.isPrimaryKey());
  }

  @Test
  public void testValidationColumnMapping_TargetColumnNoSourceMatch() {
    HashMap<String, DataType<?>> sourceCols = new HashMap<>();
    sourceCols.put("id", SQLDataType.INTEGER);

    HashMap<String, DataType<?>> targetCols = new HashMap<>();
    targetCols.put("id", SQLDataType.INTEGER);
    targetCols.put("description", SQLDataType.CLOB);

    ValidationColumnMapping mapping = new ValidationColumnMapping(ImmutableMap.of(), sourceCols, targetCols, ImmutableMap.of());
    List<ValidationColumnMapping.ColumnEntry> entries = mapping.getColumnEntries();

    assertEquals(2, entries.size());

    ValidationColumnMapping.ColumnEntry descEntry = entries.stream()
        .filter(e -> "description".equals(e.getTargetColumnName()))
        .findFirst().orElseThrow(() -> new AssertionError("Description column entry not found"));
    assertNull(descEntry.getSourceColumnName()); // No matching source column
    assertNull(descEntry.getSourceColumnDataType()); // No matching source data type
    assertEquals("description", descEntry.getTargetColumnName());
    assertEquals(SQLDataType.CLOB, descEntry.getTargetColumnDataType());
    assertFalse(descEntry.isPrimaryKey());
  }


}