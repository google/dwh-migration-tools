package com.google.edwmigration.dumper.ext.hive.metastore.utils;

import static com.google.edwmigration.dumper.ext.hive.metastore.utils.PartitionNameGenerator.makePartitionName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionNameGeneratorTest {
  @Test
  public void makePartitionName_emptySpec() {

    String partitionName = makePartitionName(ImmutableList.of(), ImmutableList.of());

    assertTrue(partitionName.isEmpty());
  }

  @Test
  public void makePartitionName_partitionWithMissedValue() {
    String partitionName = makePartitionName(ImmutableList.of("part1", "part2"),
        ImmutableList.of(""));

    assertTrue(partitionName.isEmpty());
  }

  @Test
  public void makePartitionName_onePartition() {
    String partitionName = makePartitionName(ImmutableList.of("part"), ImmutableList.of("1"));

    assertEquals("part=1", partitionName);
  }

  @Test
  public void makePartitionName_multiplePartitions() {
    String partitionName = makePartitionName(ImmutableList.of("part1", "part2", "part3"),
        ImmutableList.of("foo", "bar", "123"));

    assertEquals("part1=foo/part2=bar/part3=123", partitionName);
  }

  @Test
  public void makePartitionName_partitionWithEscape() {
    String partitionName = makePartitionName(ImmutableList.of("foo/bar", "\\baz"),
        ImmutableList.of("test^ #value", "\n\r"));

    assertEquals("foo%2Fbar=test%5E+%23value/%5Cbaz=%0A%0D", partitionName);
  }
}