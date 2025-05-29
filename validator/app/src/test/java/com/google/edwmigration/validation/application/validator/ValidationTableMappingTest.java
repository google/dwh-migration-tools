package com.google.edwmigration.validation.application.validator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;

/** @author nehanene */
@RunWith(JUnit4.class)
public class ValidationTableMappingTest {

  // --- Tests for ValidationTable class ---

  @Test
  public void testValidationTable_FourArgConstructor() {
    String fqTable = "test_schema.test_table";
    String schema = "test_schema";
    String table = "test_table";
    ValidationTableMapping.TableType type = ValidationTableMapping.TableType.SOURCE;

    ValidationTableMapping.ValidationTable validationTable =
        new ValidationTableMapping.ValidationTable(fqTable, schema, table, type);

    assertEquals(schema, validationTable.getSchema());
    assertEquals(table, validationTable.getTable());
    assertEquals(type, validationTable.getTableType());
    assertEquals(fqTable, validationTable.getFullyQualifiedTable());
    assertEquals(schema, validationTable.getDefaultSchema("default")); // Schema is not null, should return schema
  }

  @Test
  public void testValidationTable_ThreeArgConstructor() {
    String fqTable = "test_table";
    String table = "test_table";
    ValidationTableMapping.TableType type = ValidationTableMapping.TableType.TARGET;

    ValidationTableMapping.ValidationTable validationTable =
        new ValidationTableMapping.ValidationTable(fqTable, table, type);

    assertNull(validationTable.getSchema()); // Schema should be null for this constructor
    assertEquals(table, validationTable.getTable());
    assertEquals(type, validationTable.getTableType());
    assertEquals(fqTable, validationTable.getFullyQualifiedTable());
    assertEquals("default_schema_value", validationTable.getDefaultSchema("default_schema_value")); // Schema is null, should return default
  }


  // --- Tests for ValidationTableMapping class ---

  @Test
  public void testValidationTableMapping_SinglePartTableNames() {
    String source = "source_table";
    String target = "target_table";

    ValidationTableMapping mapping = new ValidationTableMapping(source, target);

    // Test SourceTable
    ValidationTableMapping.ValidationTable sourceTable = mapping.getSourceTable();
    assertEquals(source, sourceTable.getFullyQualifiedTable());
    assertNull(sourceTable.getSchema());
    assertEquals(source, sourceTable.getTable());
    assertEquals(ValidationTableMapping.TableType.SOURCE, sourceTable.getTableType());

    // Test TargetTable
    ValidationTableMapping.ValidationTable targetTable = mapping.getTargetTable();
    assertEquals(target, targetTable.getFullyQualifiedTable());
    assertNull(targetTable.getSchema());
    assertEquals(target, targetTable.getTable());
    assertEquals(ValidationTableMapping.TableType.TARGET, targetTable.getTableType());
  }

  @Test
  public void testValidationTableMapping_TwoPartTableNames() {
    String source = "source_schema.source_table";
    String target = "target_schema.target_table";

    ValidationTableMapping mapping = new ValidationTableMapping(source, target);

    // Test SourceTable
    ValidationTableMapping.ValidationTable sourceTable = mapping.getSourceTable();
    assertEquals(source, sourceTable.getFullyQualifiedTable());
    assertEquals("source_schema", sourceTable.getSchema());
    assertEquals("source_table", sourceTable.getTable());
    assertEquals(ValidationTableMapping.TableType.SOURCE, sourceTable.getTableType());

    // Test TargetTable
    ValidationTableMapping.ValidationTable targetTable = mapping.getTargetTable();
    assertEquals(target, targetTable.getFullyQualifiedTable());
    assertEquals("target_schema", targetTable.getSchema());
    assertEquals("target_table", targetTable.getTable());
    assertEquals(ValidationTableMapping.TableType.TARGET, targetTable.getTableType());
  }

  @Test
  public void testValidationTableMapping_MixedTableNames() {
    String source = "single_part_source";
    String target = "target_schema.two_part_target";

    ValidationTableMapping mapping = new ValidationTableMapping(source, target);

    // Test SourceTable (single part)
    ValidationTableMapping.ValidationTable sourceTable = mapping.getSourceTable();
    assertEquals(source, sourceTable.getFullyQualifiedTable());
    assertNull(sourceTable.getSchema());
    assertEquals("single_part_source", sourceTable.getTable());
    assertEquals(ValidationTableMapping.TableType.SOURCE, sourceTable.getTableType());

    // Test TargetTable (two part)
    ValidationTableMapping.ValidationTable targetTable = mapping.getTargetTable();
    assertEquals(target, targetTable.getFullyQualifiedTable());
    assertEquals("target_schema", targetTable.getSchema());
    assertEquals("two_part_target", targetTable.getTable());
    assertEquals(ValidationTableMapping.TableType.TARGET, targetTable.getTableType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidationTableMapping_InvalidSourceTableName() {
    new ValidationTableMapping("too.many.parts.source", "target_table");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidationTableMapping_InvalidTargetTableName() {
    new ValidationTableMapping("source_table", "too.many.parts.target");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidationTableMapping_InvalidBothTableNames() {
    new ValidationTableMapping("bad.source.name", "bad.target.name");
  }

}
